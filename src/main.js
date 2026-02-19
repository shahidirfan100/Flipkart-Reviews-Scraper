import { Actor, log } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import { gotScraping } from 'got-scraping';
import fs from 'node:fs/promises';
import path from 'node:path';
import { firefox } from 'playwright';

const FIREFOX_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 15.7; rv:147.0) Gecko/20100101 Firefox/147.0',
    'Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0',
];

const getRandomFirefoxUserAgent = () => FIREFOX_USER_AGENTS[Math.floor(Math.random() * FIREFOX_USER_AGENTS.length)];

const FLIPKART_ORIGIN = 'https://www.flipkart.com';
const ROME_API_ORIGIN = 'https://1.rome.api.flipkart.com';
const MAX_PAGES_PER_PRODUCT = 200;
const PAGE_SIZE_ESTIMATE = 10;

await Actor.init();

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseWantedCount(raw, fallback = 20) {
    const value = Number(raw);
    if (!Number.isFinite(value)) return fallback;
    return Math.max(1, Math.floor(value));
}

function cleanFlipkartUrl(inputUrl) {
    try {
        const urlObj = new URL(inputUrl, FLIPKART_ORIGIN);
        urlObj.hash = '';
        return urlObj.href;
    } catch {
        return inputUrl;
    }
}

function normalizeStartUrls(input) {
    const urls = [];
    const add = (value) => {
        if (typeof value === 'string' && value.trim()) urls.push(cleanFlipkartUrl(value.trim()));
        else if (value && typeof value.url === 'string' && value.url.trim()) urls.push(cleanFlipkartUrl(value.url.trim()));
    };

    add(input.startUrl);
    add(input.url);
    if (Array.isArray(input.startUrls)) input.startUrls.forEach(add);

    return [...new Set(urls)];
}

function toReviewUrl(inputUrl) {
    try {
        const urlObj = new URL(inputUrl, FLIPKART_ORIGIN);
        const path = urlObj.pathname;
        if (path.includes('/product-reviews/')) return urlObj.href;

        const carryQuery = new URLSearchParams();
        const pid = urlObj.searchParams.get('pid');
        const lid = urlObj.searchParams.get('lid');
        if (pid) carryQuery.set('pid', pid);
        if (lid) carryQuery.set('lid', lid);
        const queryString = carryQuery.toString();
        const suffix = queryString ? `?${queryString}` : '';

        const productMatch = path.match(/\/([^/]+)\/p\/(itm[a-z0-9]+)/i);
        if (productMatch) {
            return `${FLIPKART_ORIGIN}/${productMatch[1]}/product-reviews/${productMatch[2]}${suffix}`;
        }

        const itmMatch = path.match(/(itm[a-z0-9]+)/i);
        if (itmMatch) {
            const parts = path.split('/').filter(Boolean);
            const productSlug = parts[0] || 'product';
            return `${FLIPKART_ORIGIN}/${productSlug}/product-reviews/${itmMatch[1]}${suffix}`;
        }

        return urlObj.href;
    } catch {
        return inputUrl;
    }
}

function buildPagedUrl(baseUrl, page) {
    const urlObj = new URL(baseUrl, FLIPKART_ORIGIN);
    if (page > 1) urlObj.searchParams.set('page', String(page));
    else urlObj.searchParams.delete('page');
    return urlObj.href;
}

function isValidPid(pid) {
    return typeof pid === 'string' && /^[A-Z0-9]{10,24}$/i.test(pid);
}

function extractIdentifiersFromUrl(inputUrl) {
    try {
        const urlObj = new URL(inputUrl, FLIPKART_ORIGIN);
        const pidRaw = urlObj.searchParams.get('pid');
        const lidRaw = urlObj.searchParams.get('lid');
        const pid = isValidPid(pidRaw) ? pidRaw.toUpperCase() : null;
        const lid = typeof lidRaw === 'string' && lidRaw.trim() ? lidRaw.trim() : null;
        return { pid, lid };
    } catch {
        return { pid: null, lid: null };
    }
}

function buildRomeReviewApiUrl({ pid, lid, page }) {
    const urlObj = new URL('/api/3/product/reviews', ROME_API_ORIGIN);
    urlObj.searchParams.set('pid', pid);
    urlObj.searchParams.set('aid', 'overall');
    urlObj.searchParams.set('sortOrder', 'MOST_HELPFUL');
    urlObj.searchParams.set('certifiedBuyer', 'false');
    urlObj.searchParams.set('page', String(page));
    if (lid) urlObj.searchParams.set('lid', lid);
    return urlObj.href;
}

function extractInitialStateFromHtml(html) {
    const marker = html.match(/window\.__INITIAL_STATE__\s*=\s*/);
    if (!marker || marker.index == null) return null;

    let start = marker.index + marker[0].length;
    while (start < html.length && html[start] !== '{') start++;
    if (start >= html.length) return null;

    let depth = 0;
    let inString = false;
    let escaped = false;
    let end = -1;

    for (let i = start; i < html.length; i++) {
        const ch = html[i];
        if (inString) {
            if (escaped) escaped = false;
            else if (ch === '\\') escaped = true;
            else if (ch === '"') inString = false;
            continue;
        }

        if (ch === '"') {
            inString = true;
            continue;
        }

        if (ch === '{') depth++;
        else if (ch === '}') {
            depth--;
            if (depth === 0) {
                end = i + 1;
                break;
            }
        }
    }

    if (end === -1) return null;

    try {
        return JSON.parse(html.slice(start, end));
    } catch (error) {
        log.warning(`Failed to parse __INITIAL_STATE__: ${error.message}`);
        return null;
    }
}

function extractNextDataFromHtml(html) {
    const match = html.match(/<script[^>]*id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/i);
    if (!match) return null;
    try {
        const parsed = JSON.parse(match[1]);
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch (error) {
        log.warning(`Failed to parse __NEXT_DATA__: ${error.message}`);
        return null;
    }
}

function extractStructuredStateFromHtml(html) {
    return extractInitialStateFromHtml(html) || extractNextDataFromHtml(html);
}

async function extractStructuredStateWithPlaywright(page, urlForDebug) {
    await page.waitForLoadState('domcontentloaded').catch(() => undefined);
    await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => undefined);
    await page.waitForTimeout(750).catch(() => undefined);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => undefined);
    await page.waitForTimeout(750).catch(() => undefined);

    const state = await page
        .evaluate(() => window.__INITIAL_STATE__ || window.__NEXT_DATA__ || null)
        .catch(() => null);
    if (state && typeof state === 'object') return state;

    const html = await page.content().catch(() => null);
    if (!html) return null;
    const parsed = extractStructuredStateFromHtml(html);
    if (!parsed) {
        await Actor.setValue(`debug-playwright-missing-state-${Date.now()}.html`, html, { contentType: 'text/html' }).catch(() => undefined);
        log.warning(`Playwright: structured state not found at ${urlForDebug}`);
    }
    return parsed;
}

async function runPlaywrightFirefoxFallback({
    reviewBaseUrl,
    pagesToTry,
    resultsWanted,
    proxyConfiguration,
    seenReviewIds,
    pushReview,
    isNearTimeout,
    shouldStop,
}) {
    const startUrls = [];
    for (let page = 1; page <= pagesToTry; page++) {
        const pageUrl = buildPagedUrl(reviewBaseUrl, page);
        startUrls.push({ url: pageUrl, userData: { page } });
    }

    const crawler = new PlaywrightCrawler({
        launchContext: {
            launcher: firefox,
            launchOptions: {
                headless: true,
            },
            userAgent: getRandomFirefoxUserAgent(),
        },
        proxyConfiguration,
        maxConcurrency: 5,
        maxRequestRetries: 1,
        navigationTimeoutSecs: 30,
        requestHandlerTimeoutSecs: 45,
        preNavigationHooks: [
            async ({ page }) => {
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    const url = route.request().url();

                    if (
                        ['image', 'font', 'media', 'stylesheet'].includes(type)
                        || url.includes('google-analytics')
                        || url.includes('googletagmanager')
                        || url.includes('facebook')
                        || url.includes('doubleclick')
                        || url.includes('adsense')
                    ) {
                        return route.abort();
                    }
                    return route.continue();
                });
            },
        ],
        requestHandler: async ({ page, request }) => {
            if (isNearTimeout()) {
                log.warning('Playwright: near timeout, skipping remaining pages.');
                return;
            }

            if (shouldStop()) return;

            log.debug(`Playwright(Firefox) processing: ${request.url}`);
            await page.waitForSelector('body', { timeout: 10000 }).catch(() => undefined);

            const state = await extractStructuredStateWithPlaywright(page, request.url);
            if (!state) return;

            const extracted = extractReviewsFromState(state, request.url);
            log.debug(`Playwright page ${request.userData?.page ?? '?'}: extracted ${extracted.length} reviews from structured state.`);

            for (const review of extracted) {
                if (seenReviewIds.has(review.review_id)) continue;
                if (shouldStop()) break;
                seenReviewIds.add(review.review_id);
                await pushReview(review);
            }
        },
    });

    await crawler.run(startUrls);
}

function scoreReviewUrlCandidate(path) {
    let score = 0;
    if (path.includes('pid=')) score += 6;
    if (path.includes('lid=')) score += 3;
    if (path.includes('aid=overall')) score += 4;
    if (path.includes('sortOrder=')) score += 2;
    if (path.includes('certifiedBuyer=')) score += 1;
    return score;
}

function extractCanonicalReviewUrl(state, fallbackUrl) {
    const candidates = [];

    const walk = (node) => {
        if (!node) return;
        if (Array.isArray(node)) {
            node.forEach(walk);
            return;
        }
        if (typeof node !== 'object') return;

        if (typeof node.url === 'string' && node.url.includes('/product-reviews/')) candidates.push(node.url);
        if (node.action && typeof node.action.url === 'string' && node.action.url.includes('/product-reviews/')) {
            candidates.push(node.action.url);
        }

        Object.values(node).forEach(walk);
    };

    walk(state);
    if (!candidates.length) return fallbackUrl;

    const best = candidates.sort((a, b) => scoreReviewUrlCandidate(b) - scoreReviewUrlCandidate(a))[0];
    const absolute = new URL(best, FLIPKART_ORIGIN);
    absolute.searchParams.delete('page');
    return absolute.href;
}

function findProductContextFromUrl(url) {
    try {
        const parsed = new URL(url, FLIPKART_ORIGIN);
        const pathParts = parsed.pathname.split('/').filter(Boolean);
        const idx = pathParts.findIndex((p) => p === 'product-reviews');
        const productSlug = idx > 0 ? pathParts[idx - 1] : (pathParts[0] || 'product');
        const productName = decodeURIComponent(productSlug).replace(/-/g, ' ').trim();
        const productIdFromQuery = parsed.searchParams.get('pid');
        return { productName, productIdFromQuery };
    } catch {
        return { productName: null, productIdFromQuery: null };
    }
}

function parseProductIdFromReviewUrl(reviewUrl) {
    if (!reviewUrl) return null;
    const match = reviewUrl.match(/\/reviews\/([^:/?]+):/i);
    return match ? match[1] : null;
}

function normalizeImageUrl(urlTemplate) {
    if (!urlTemplate || typeof urlTemplate !== 'string') return null;
    return urlTemplate
        .replaceAll('{@width}', '1280')
        .replaceAll('{@height}', '1280')
        .replaceAll('{@quality}', '100');
}

function extractImageUrls(reviewValue) {
    const images = [];
    const addImage = (candidate) => {
        const normalized = normalizeImageUrl(candidate);
        if (normalized) images.push(normalized);
    };

    const sources = [
        ...(Array.isArray(reviewValue.images) ? reviewValue.images : []),
        ...(Array.isArray(reviewValue.media) ? reviewValue.media : []),
    ];

    for (const image of sources) {
        if (typeof image?.imageURL === 'string') addImage(image.imageURL);
        if (typeof image?.value?.imageURL === 'string') addImage(image.value.imageURL);
    }

    return [...new Set(images)];
}

function extractReviewsFromState(state, sourcePageUrl) {
    const reviews = [];
    const seen = new Set();
    const { productName, productIdFromQuery } = findProductContextFromUrl(sourcePageUrl);

    const walk = (node) => {
        if (!node) return;
        if (Array.isArray(node)) {
            node.forEach(walk);
            return;
        }
        if (typeof node !== 'object') return;

        if (node.type === 'ProductReviewValue' && typeof node.id === 'string') {
            if (!seen.has(node.id)) {
                seen.add(node.id);
                const sourceReviewUrl = typeof node.url === 'string' ? new URL(node.url, FLIPKART_ORIGIN).href : sourcePageUrl;
                const rating = Number(node.rating);
                const helpfulCount = Number(node.helpfulCount ?? node.upvote?.value?.count ?? 0);
                const productId = parseProductIdFromReviewUrl(node.url) || productIdFromQuery;

                reviews.push({
                    product_name: productName,
                    product_id: productId,
                    review_id: node.id,
                    rating: Number.isFinite(rating) ? rating : null,
                    title: node.title || null,
                    review_text: node.text || null,
                    author: node.author || null,
                    date: node.created || null,
                    verified_purchase: Boolean(node.certifiedBuyer || node.reviewPropertyMap?.VERIFIED_PURCHASE),
                    helpful_count: Number.isFinite(helpfulCount) ? helpfulCount : 0,
                    review_images: extractImageUrls(node),
                    location: node.location?.city && node.location?.state
                        ? `${node.location.city}, ${node.location.state}`
                        : null,
                    url: sourceReviewUrl,
                });
            }
        }

        Object.values(node).forEach(walk);
    };

    walk(state);
    return reviews;
}

function tryParseJson(body) {
    if (typeof body !== 'string' || !body.trim()) return null;
    try {
        return JSON.parse(body);
    } catch {
        return null;
    }
}

function extractReviewsFromLoosePayload(payload, sourcePageUrl) {
    const reviews = [];
    const seen = new Set();
    const { productName, productIdFromQuery } = findProductContextFromUrl(sourcePageUrl);

    const walk = (node) => {
        if (!node) return;
        if (Array.isArray(node)) {
            node.forEach(walk);
            return;
        }
        if (typeof node !== 'object') return;

        const maybeId = typeof node.id === 'string' ? node.id : (typeof node.reviewId === 'string' ? node.reviewId : null);
        const maybeText = typeof node.text === 'string' ? node.text : (typeof node.reviewText === 'string' ? node.reviewText : null);
        const maybeTitle = typeof node.title === 'string' ? node.title : (typeof node.reviewTitle === 'string' ? node.reviewTitle : null);
        const maybeAuthor = typeof node.author === 'string' ? node.author : (typeof node.userName === 'string' ? node.userName : null);
        const maybeRatingRaw = node.rating ?? node.stars ?? node.star;
        const maybeRating = Number(maybeRatingRaw);

        if (maybeId && (maybeText || maybeTitle) && Number.isFinite(maybeRating)) {
            if (!seen.has(maybeId)) {
                seen.add(maybeId);
                reviews.push({
                    product_name: productName,
                    product_id: productIdFromQuery,
                    review_id: maybeId,
                    rating: maybeRating,
                    title: maybeTitle || null,
                    review_text: maybeText || null,
                    author: maybeAuthor || null,
                    date: node.created || node.date || null,
                    verified_purchase: Boolean(node.certifiedBuyer || node.verifiedPurchase),
                    helpful_count: Number(node.helpfulCount ?? node.helpfulVotes ?? 0) || 0,
                    review_images: extractImageUrls(node),
                    location: null,
                    url: sourcePageUrl,
                });
            }
        }

        Object.values(node).forEach(walk);
    };

    walk(payload);
    return reviews;
}

function extractReviewsFromAnyPayload(payload, sourceUrl) {
    const structured = extractReviewsFromState(payload, sourceUrl);
    if (structured.length) return structured;
    return extractReviewsFromLoosePayload(payload, sourceUrl);
}

function isLikelyReviewApiEndpoint(url) {
    try {
        const parsed = new URL(url, FLIPKART_ORIGIN);
        const host = parsed.hostname.toLowerCase();
        const path = parsed.pathname.toLowerCase();
        const full = parsed.href.toLowerCase();
        const isFlipkartInfra = host.includes('flipkart.com') || host.includes('flixcart.com');
        if (!isFlipkartInfra) return false;
        const isApiPath = path.includes('/api/') || path.includes('/graphql');
        const hasReviewHint = full.includes('review') || full.includes('aid=overall') || full.includes('product-reviews');
        return isApiPath && hasReviewHint;
    } catch {
        return false;
    }
}

function scoreReviewApiCandidate({ url, method, status, extractedCount }) {
    const lower = (url || '').toLowerCase();
    let score = 0;
    if (status >= 200 && status < 300) score += 20;
    if ((method || '').toUpperCase() === 'GET') score += 4;
    if (lower.includes('/product/reviews')) score += 40;
    if (lower.includes('aid=overall')) score += 10;
    if (lower.includes('pid=')) score += 8;
    if (lower.includes('lid=')) score += 4;
    if (lower.includes('page=')) score += 6;
    score += Math.min(25, extractedCount * 3);
    return score;
}

function sanitizeBrowserFetchHeaders(headers = {}) {
    const output = {};
    const blocked = new Set([
        'host',
        'connection',
        'content-length',
        'content-encoding',
        'cookie',
        'origin',
        'referer',
        'user-agent',
        'accept-encoding',
    ]);

    for (const [keyRaw, value] of Object.entries(headers)) {
        const key = String(keyRaw).toLowerCase();
        if (!value || blocked.has(key) || key.startsWith('sec-')) continue;
        if (key === 'accept' || key === 'content-type' || key.startsWith('x-')) {
            output[key] = String(value);
        }
    }

    return output;
}

function parseJsonObjectOrNull(value) {
    if (typeof value !== 'string' || !value.trim()) return null;
    try {
        const parsed = JSON.parse(value);
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch {
        return null;
    }
}

function inferPaginationStrategy(url, postData) {
    try {
        const parsedUrl = new URL(url, FLIPKART_ORIGIN);
        if (parsedUrl.searchParams.has('page')) {
            const start = Number(parsedUrl.searchParams.get('page') || 1);
            return { type: 'query-page', key: 'page', start: Number.isFinite(start) ? start : 1 };
        }
        if (parsedUrl.searchParams.has('offset')) {
            const start = Number(parsedUrl.searchParams.get('offset') || 0);
            const limit = Number(parsedUrl.searchParams.get('limit') || PAGE_SIZE_ESTIMATE);
            return {
                type: 'query-offset',
                key: 'offset',
                start: Number.isFinite(start) ? start : 0,
                step: Number.isFinite(limit) && limit > 0 ? limit : PAGE_SIZE_ESTIMATE,
            };
        }
    } catch {
        // Ignore URL parse failures and continue to body strategy.
    }

    const postDataObject = parseJsonObjectOrNull(postData);
    if (postDataObject && typeof postDataObject.page === 'number') {
        return { type: 'body-page', key: 'page', start: postDataObject.page };
    }
    if (postDataObject && typeof postDataObject.offset === 'number') {
        const step = typeof postDataObject.limit === 'number' && postDataObject.limit > 0
            ? postDataObject.limit
            : PAGE_SIZE_ESTIMATE;
        return { type: 'body-offset', key: 'offset', start: postDataObject.offset, step };
    }

    return null;
}

function buildApiContractFromCandidate(candidate) {
    if (!candidate?.url || !candidate?.method) return null;
    const pagination = inferPaginationStrategy(candidate.url, candidate.postData);
    return {
        method: candidate.method.toUpperCase(),
        url: candidate.url,
        headers: sanitizeBrowserFetchHeaders(candidate.requestHeaders),
        postData: candidate.postData || null,
        pagination,
    };
}

function buildRequestFromApiContract(contract, pageNumber) {
    const method = (contract?.method || 'GET').toUpperCase();
    const requestUrl = new URL(contract.url, FLIPKART_ORIGIN);
    let postDataObject = parseJsonObjectOrNull(contract.postData);

    if (pageNumber > 1) {
        if (!contract.pagination) return null;

        if (contract.pagination.type === 'query-page') {
            requestUrl.searchParams.set(contract.pagination.key, String(pageNumber));
        } else if (contract.pagination.type === 'query-offset') {
            const offset = (pageNumber - 1) * contract.pagination.step;
            requestUrl.searchParams.set(contract.pagination.key, String(offset));
        } else if (contract.pagination.type === 'body-page') {
            if (!postDataObject) return null;
            postDataObject[contract.pagination.key] = pageNumber;
        } else if (contract.pagination.type === 'body-offset') {
            if (!postDataObject) return null;
            postDataObject[contract.pagination.key] = (pageNumber - 1) * contract.pagination.step;
        } else {
            return null;
        }
    }

    const body = method === 'GET' || method === 'HEAD' || !postDataObject
        ? undefined
        : JSON.stringify(postDataObject);

    return {
        url: requestUrl.href,
        method,
        headers: contract.headers || {},
        body,
    };
}

function toPlaywrightProxyOptions(proxyUrl) {
    if (!proxyUrl) return null;
    try {
        const parsed = new URL(proxyUrl);
        const server = `${parsed.protocol}//${parsed.hostname}${parsed.port ? `:${parsed.port}` : ''}`;
        const options = { server };
        if (parsed.username) options.username = decodeURIComponent(parsed.username);
        if (parsed.password) options.password = decodeURIComponent(parsed.password);
        return options;
    } catch {
        return null;
    }
}

function toStableReviewId(review) {
    if (typeof review?.review_id === 'string' && review.review_id.trim()) return review.review_id.trim();
    const raw = `${review?.author || ''}|${review?.title || ''}|${review?.review_text || ''}|${review?.date || ''}|${review?.rating || ''}`;
    if (!raw.trim()) return null;
    return `gen_${Buffer.from(raw).toString('base64url').slice(0, 48)}`;
}

async function hasPlaywrightFirefoxExecutable() {
    try {
        const executablePath = firefox.executablePath();
        await fs.access(executablePath);
        return true;
    } catch {
        return false;
    }
}

async function discoverAndFetchReviewsWithDynamicApi({
    reviewUrl,
    pagesToTry,
    maxReviews,
    proxyConfiguration,
    isNearTimeout,
}) {
    let browser;
    let context;
    let page;

    const discoveredReviews = [];
    const discoveredReviewIds = new Set();
    let bestCandidate = null;
    let bestScore = -1;

    if (!(await hasPlaywrightFirefoxExecutable())) {
        log.debug('Dynamic API discovery skipped: Playwright Firefox executable is missing in this runtime image.');
        return { reviews: [], contract: null };
    }

    const addReviews = (reviews) => {
        for (const review of reviews) {
            if (discoveredReviews.length >= maxReviews) break;
            const reviewId = toStableReviewId(review);
            if (!reviewId || discoveredReviewIds.has(reviewId)) continue;
            discoveredReviewIds.add(reviewId);
            discoveredReviews.push({ ...review, review_id: reviewId });
        }
    };

    try {
        const proxyUrl = proxyConfiguration ? await proxyConfiguration.newUrl() : null;
        const proxyOptions = toPlaywrightProxyOptions(proxyUrl);
        const launchOptions = {
            headless: true,
            ...(proxyOptions ? { proxy: proxyOptions } : {}),
        };

        browser = await firefox.launch(launchOptions);
        context = await browser.newContext({
            userAgent: getRandomFirefoxUserAgent(),
            locale: 'en-US',
        });
        page = await context.newPage();

        const handleResponse = async (response) => {
            try {
                const request = response.request();
                if (!['xhr', 'fetch'].includes(request.resourceType())) return;
                const url = response.url();
                if (!isLikelyReviewApiEndpoint(url)) return;

                const status = response.status();
                const method = request.method().toUpperCase();
                const requestHeaders = request.headers();
                const postData = request.postData();

                let extractedReviews = [];
                if (status >= 200 && status < 400) {
                    const body = await response.text().catch(() => null);
                    const parsed = tryParseJson(body || '');
                    if (parsed) extractedReviews = extractReviewsFromAnyPayload(parsed, url);
                }

                const candidate = {
                    url,
                    method,
                    status,
                    extractedCount: extractedReviews.length,
                    requestHeaders,
                    postData,
                };
                const score = scoreReviewApiCandidate(candidate);
                if (score > bestScore) {
                    bestScore = score;
                    bestCandidate = candidate;
                }

                if (extractedReviews.length) {
                    addReviews(extractedReviews);
                }
            } catch {
                // Ignore parse and capture errors from noisy network traffic.
            }
        };

        page.on('response', (response) => {
            void handleResponse(response);
        });

        await page.goto(reviewUrl, { waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => null);
        await page.waitForTimeout(2500).catch(() => undefined);
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => undefined);
        await page.waitForTimeout(2500).catch(() => undefined);

        const contract = buildApiContractFromCandidate(bestCandidate);
        if (!contract) {
            return { reviews: discoveredReviews, contract: null };
        }

        log.debug(`Dynamic API contract discovered: ${contract.method} ${contract.url}`);

        let noNewPages = 0;
        for (let pageNumber = 2; pageNumber <= pagesToTry; pageNumber++) {
            if (discoveredReviews.length >= maxReviews || isNearTimeout()) break;

            const request = buildRequestFromApiContract(contract, pageNumber);
            if (!request) break;

            const apiResult = await page.evaluate(async (req) => {
                try {
                    const init = {
                        method: req.method,
                        headers: req.headers || {},
                        credentials: 'include',
                    };
                    if (req.body && req.method !== 'GET' && req.method !== 'HEAD') {
                        init.body = req.body;
                    }
                    const response = await fetch(req.url, init);
                    const body = await response.text();
                    return {
                        status: response.status,
                        body,
                        contentType: response.headers.get('content-type') || '',
                    };
                } catch (error) {
                    return { status: 0, error: String(error) };
                }
            }, request);

            if (!apiResult || apiResult.status >= 400 || apiResult.status === 0) {
                break;
            }

            const parsed = tryParseJson(apiResult.body || '');
            if (!parsed) break;

            const extractedReviews = extractReviewsFromAnyPayload(parsed, request.url);
            if (!extractedReviews.length) {
                noNewPages++;
                if (noNewPages >= 2) break;
                continue;
            }

            const before = discoveredReviews.length;
            addReviews(extractedReviews);
            const added = discoveredReviews.length - before;
            if (added === 0) {
                noNewPages++;
                if (noNewPages >= 2) break;
            } else {
                noNewPages = 0;
            }
        }

        return { reviews: discoveredReviews, contract };
    } catch (error) {
        log.warning(`Dynamic API discovery failed: ${error?.message || error}`);
        return { reviews: [], contract: null };
    } finally {
        await page?.close().catch(() => undefined);
        await context?.close().catch(() => undefined);
        await browser?.close().catch(() => undefined);
    }
}

function isLikelyBlocked(html) {
    const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
    const title = (titleMatch?.[1] || '').trim().toLowerCase();
    return (
        title.includes('access denied')
        || title.includes('captcha')
        || title.includes('robot')
        || html.toLowerCase().includes('datadome')
        || html.toLowerCase().includes('cf_chl')
    );
}

async function fetchReviewPageHtml(url, proxyConfiguration) {
    const requestHeaders = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'cache-control': 'no-cache',
        pragma: 'no-cache',
        referer: FLIPKART_ORIGIN,
    };

    return fetchWithProxyRetries({
        url,
        proxyConfiguration,
        headers: requestHeaders,
        timeoutMs: 25000,
        maxProxyAttempts: 4,
        retryDirectWhenProxyFails: true,
    });
}

function isProxyTransportError(error) {
    const message = `${error?.message || ''} ${error?.cause?.message || ''}`.toLowerCase();
    return (
        message.includes('proxy responded with 595')
        || message.includes('econnreset')
        || message.includes('econnrefused')
        || message.includes('etimedout')
        || message.includes('socket hang up')
        || message.includes('proxy')
    );
}

async function fetchWithProxyRetries({
    url,
    proxyConfiguration,
    headers,
    timeoutMs = 25000,
    maxProxyAttempts = 4,
    retryDirectWhenProxyFails = false,
}) {
    const requestOnce = async (proxyUrl) => {
        const response = await gotScraping({
            url,
            proxyUrl,
            timeout: { request: timeoutMs },
            retry: { limit: 1 },
            throwHttpErrors: false,
            http2: false,
            headers,
        });

        return {
            statusCode: response.statusCode,
            body: response.body,
            headers: response.headers,
        };
    };

    if (!proxyConfiguration) {
        return requestOnce(undefined);
    }

    let lastResponse;
    let lastTransportError;
    for (let attempt = 1; attempt <= maxProxyAttempts; attempt++) {
        try {
            const proxyUrl = await proxyConfiguration.newUrl();
            const response = await requestOnce(proxyUrl);
            lastResponse = response;

            // Rotate proxy session on common temporary blocks.
            if ([403, 429, 503].includes(response.statusCode) && attempt < maxProxyAttempts) {
                log.debug(`Proxy attempt ${attempt}/${maxProxyAttempts} got HTTP ${response.statusCode}, retrying with new proxy session.`);
                continue;
            }

            return response;
        } catch (error) {
            lastTransportError = error;
            if (isProxyTransportError(error) && attempt < maxProxyAttempts) {
                log.debug(`Proxy transport failed (attempt ${attempt}/${maxProxyAttempts}) for ${url}, retrying with new proxy session.`);
                continue;
            }
            throw error;
        }
    }

    if (retryDirectWhenProxyFails) {
        log.debug(`All proxy attempts failed for ${url}. Retrying direct once.`);
        return requestOnce(undefined);
    }

    if (lastTransportError) throw lastTransportError;
    if (lastResponse) return lastResponse;
    throw new Error(`Failed to fetch URL after proxy retries: ${url}`);
}

async function fetchReviewApiJson(url, proxyConfiguration) {
    const requestHeaders = {
        'user-agent': 'Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro Build/UQ1A.240205.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36',
        accept: 'application/json,text/plain,*/*',
        'accept-language': 'en-US,en;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'cache-control': 'no-cache',
        pragma: 'no-cache',
        referer: FLIPKART_ORIGIN,
        origin: FLIPKART_ORIGIN,
        'x-requested-with': 'XMLHttpRequest',
        'x-user-agent': 'FKUA/website/42/website/Desktop',
    };

    return fetchWithProxyRetries({
        url,
        proxyConfiguration,
        headers: requestHeaders,
        timeoutMs: 30000,
        maxProxyAttempts: 4,
        retryDirectWhenProxyFails: false,
    });
}

function hasAnyStartUrl(input) {
    if (!input || typeof input !== 'object') return false;
    if (typeof input.startUrl === 'string' && input.startUrl.trim()) return true;
    if (typeof input.url === 'string' && input.url.trim()) return true;
    if (Array.isArray(input.startUrls) && input.startUrls.some((u) => (typeof u === 'string' && u.trim()) || (u && typeof u.url === 'string' && u.url.trim()))) {
        return true;
    }
    return false;
}

async function loadBundledInputJson() {
    try {
        const inputPath = path.join(process.cwd(), 'INPUT.json');
        const raw = await fs.readFile(inputPath, 'utf8');
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch {
        return null;
    }
}

function isProxyActuallyEnabled(proxyConfigurationInput) {
    if (!proxyConfigurationInput || typeof proxyConfigurationInput !== 'object') return false;
    if (proxyConfigurationInput.useApifyProxy === true) return true;
    if (Array.isArray(proxyConfigurationInput.proxyUrls) && proxyConfigurationInput.proxyUrls.length > 0) return true;
    return false;
}

async function main() {
    const env = Actor.getEnv();
    const timeoutAt = typeof env?.timeoutAt === 'number' ? env.timeoutAt : null;
    const isNearTimeout = () => (timeoutAt ? (Date.now() + 25000 >= timeoutAt) : false);
    const isAtHome = Boolean(env?.isAtHome);

    let input = (await Actor.getInput()) || {};
    if (!hasAnyStartUrl(input)) {
        const bundledInput = await loadBundledInputJson();
        if (bundledInput && hasAnyStartUrl(bundledInput)) {
            log.debug('Input is empty; falling back to bundled INPUT.json defaults.');
            const sanitizedInput = { ...input };
            if (typeof sanitizedInput.startUrl === 'string' && !sanitizedInput.startUrl.trim()) delete sanitizedInput.startUrl;
            if (typeof sanitizedInput.url === 'string' && !sanitizedInput.url.trim()) delete sanitizedInput.url;
            if (Array.isArray(sanitizedInput.startUrls) && sanitizedInput.startUrls.length === 0) delete sanitizedInput.startUrls;
            input = { ...bundledInput, ...sanitizedInput };
        }
    }

    const resultsWanted = parseWantedCount(input.results_wanted, 20);
    const maxPagesPerProduct = Math.min(MAX_PAGES_PER_PRODUCT, parseWantedCount(input.max_pages ?? input.maxPages, 20));
    const preferApi = true;
    const failOnEmpty = false;
    const startUrls = normalizeStartUrls(input);

    if (!startUrls.length) {
        throw new Error('No start URL provided. Please provide a Flipkart product/review URL.');
    }

    const proxyConfiguration = isProxyActuallyEnabled(input.proxyConfiguration)
        ? await Actor.createProxyConfiguration({ ...input.proxyConfiguration })
        : undefined;

    let fallbackProxyConfiguration;
    const getFallbackProxyConfiguration = async () => {
        if (fallbackProxyConfiguration !== undefined) return fallbackProxyConfiguration;

        // Don't attempt Apify Proxy locally unless credentials are available.
        if (!isAtHome) {
            fallbackProxyConfiguration = null;
            return null;
        }

        try {
            log.debug('Attempting Apify Proxy fallback for improved reliability...');
            fallbackProxyConfiguration = await Actor.createProxyConfiguration({ useApifyProxy: true });
            return fallbackProxyConfiguration;
        } catch (error) {
            fallbackProxyConfiguration = null;
            log.warning(`Apify Proxy fallback is not available: ${error?.message || error}`);
            return null;
        }
    };

    let totalSaved = 0;
    const seenReviewIds = new Set();
    let batchBuffer = [];

    const flushBatch = async (force = false) => {
        if (batchBuffer.length >= 25 || (force && batchBuffer.length > 0)) {
            const pushedCount = batchBuffer.length;
            await Actor.pushData(batchBuffer);
            batchBuffer = [];
            log.info(`Pushed ${pushedCount} reviews (total: ${totalSaved}/${resultsWanted})`);
        }
    };

    for (const sourceUrl of startUrls) {
        if (totalSaved >= resultsWanted) break;

        let canonicalReviewUrl = toReviewUrl(sourceUrl);
        const sourceIdentifiers = extractIdentifiersFromUrl(sourceUrl);
        const reviewIdentifiers = extractIdentifiersFromUrl(canonicalReviewUrl);
        let knownPid = sourceIdentifiers.pid || reviewIdentifiers.pid;
        let knownLid = sourceIdentifiers.lid || reviewIdentifiers.lid;
        let noNewDataPages = 0;

        log.info(`Processing: ${canonicalReviewUrl}`);

        if (preferApi && totalSaved < resultsWanted && !isNearTimeout()) {
            const remaining = resultsWanted - totalSaved;
            const dynamicPagesToTry = Math.min(
                maxPagesPerProduct,
                Math.max(2, Math.ceil(remaining / PAGE_SIZE_ESTIMATE) + 2),
                20,
            );

            const dynamicResult = await discoverAndFetchReviewsWithDynamicApi({
                reviewUrl: canonicalReviewUrl,
                pagesToTry: dynamicPagesToTry,
                maxReviews: remaining,
                proxyConfiguration,
                isNearTimeout,
            });

            if (dynamicResult.contract) {
                const discoveredIdentifiers = extractIdentifiersFromUrl(dynamicResult.contract.url);
                knownPid = knownPid || discoveredIdentifiers.pid;
                knownLid = knownLid || discoveredIdentifiers.lid;
            }

            if (dynamicResult.reviews.length) {
                log.debug(`Dynamic API discovery collected ${dynamicResult.reviews.length} reviews.`);
                for (const review of dynamicResult.reviews) {
                    if (totalSaved >= resultsWanted) break;
                    if (seenReviewIds.has(review.review_id)) continue;
                    seenReviewIds.add(review.review_id);
                    batchBuffer.push(review);
                    totalSaved++;
                }
                await flushBatch();
            } else {
                log.debug('Dynamic API discovery found no extractable reviews.');
            }
        }

        if (totalSaved >= resultsWanted) break;

        if (preferApi && !knownPid) {
            log.debug('API-first skipped: no pid was found in input URL. Falling back to structured page extraction.');
        }

        if (preferApi && knownPid && totalSaved < resultsWanted) {
            log.debug(`API-first mode enabled for pid=${knownPid}${knownLid ? `, lid=${knownLid}` : ''}`);
            let apiNoNewDataPages = 0;

            for (let page = 1; page <= maxPagesPerProduct && totalSaved < resultsWanted; page++) {
                if (isNearTimeout()) {
                    log.warning('Run is close to timeout; stopping API pagination early to flush results.');
                    break;
                }

                const apiUrl = buildRomeReviewApiUrl({ pid: knownPid, lid: knownLid, page });
                log.debug(`Fetching API page ${page}: ${apiUrl}`);

                let body;
                let statusCode;
                let usedFallbackProxy = false;

                try {
                    ({ body, statusCode } = await fetchReviewApiJson(apiUrl, proxyConfiguration));
                } catch (error) {
                    log.debug(`API request failed at page ${page}: ${error?.message || error}`);
                    break;
                }

                const shouldRetryWithProxy = !proxyConfiguration && !usedFallbackProxy && (statusCode === 403 || statusCode === 429 || statusCode === 533);
                if (shouldRetryWithProxy) {
                    const fallbackProxy = await getFallbackProxyConfiguration();
                    if (fallbackProxy) {
                        usedFallbackProxy = true;
                        ({ body, statusCode } = await fetchReviewApiJson(apiUrl, fallbackProxy));
                    }
                }

                if (statusCode >= 400) {
                    log.debug(`API page ${page} returned HTTP ${statusCode}. Falling back to page extraction.`);
                    break;
                }

                const parsed = tryParseJson(body);
                if (!parsed) {
                    log.debug(`API page ${page} did not return valid JSON. Falling back to page extraction.`);
                    break;
                }

                let extractedReviews = extractReviewsFromState(parsed, apiUrl);
                if (!extractedReviews.length) extractedReviews = extractReviewsFromLoosePayload(parsed, apiUrl);
                log.debug(`API page ${page}: extracted ${extractedReviews.length} reviews.`);

                if (!extractedReviews.length) {
                    apiNoNewDataPages++;
                    if (page === 1 || apiNoNewDataPages >= 2) break;
                    continue;
                }

                let newCount = 0;
                for (const review of extractedReviews) {
                    if (totalSaved >= resultsWanted) break;
                    if (seenReviewIds.has(review.review_id)) continue;

                    seenReviewIds.add(review.review_id);
                    batchBuffer.push(review);
                    totalSaved++;
                    newCount++;
                }

                await flushBatch();

                if (newCount === 0) apiNoNewDataPages++;
                else apiNoNewDataPages = 0;

                if (apiNoNewDataPages >= 2) break;
                if (extractedReviews.length < PAGE_SIZE_ESTIMATE) break;

                await sleep(500 + Math.floor(Math.random() * 400));
            }
        }

        if (totalSaved >= resultsWanted) break;

        let shouldUsePlaywrightForProduct = false;

        for (let page = 1; page <= maxPagesPerProduct && totalSaved < resultsWanted; page++) {
            if (isNearTimeout()) {
                log.warning('Run is close to timeout; stopping pagination early to flush results.');
                break;
            }

            const pageUrl = buildPagedUrl(canonicalReviewUrl, page);
            log.debug(`Fetching page ${page}: ${pageUrl}`);

            const fetchPage = (url, proxyConfig) => fetchReviewPageHtml(url, proxyConfig);
            let body;
            let statusCode;
            let usedFallbackProxy = false;

            try {
                ({ body, statusCode } = await fetchPage(pageUrl, proxyConfiguration));
            } catch (error) {
                log.debug(`Fetch failed at page ${page}: ${error?.message || error}`);
                // If the first page fetch fails, try Playwright fallback for this product.
                if (page === 1) {
                    shouldUsePlaywrightForProduct = true;
                }
                break;
            }

            const shouldRetryWithProxy = !proxyConfiguration && !usedFallbackProxy && (statusCode === 403 || statusCode === 429);
            if (shouldRetryWithProxy) {
                const fallbackProxy = await getFallbackProxyConfiguration();
                if (fallbackProxy) {
                    usedFallbackProxy = true;
                    ({ body, statusCode } = await fetchPage(pageUrl, fallbackProxy));
                }
            }

            if (statusCode >= 400) {
                log.debug(`Skipping page ${page}, HTTP ${statusCode}: ${pageUrl}`);
                if (page === 1 && [403, 429, 503].includes(statusCode)) {
                    shouldUsePlaywrightForProduct = true;
                }
                break;
            }

            if (isLikelyBlocked(body)) {
                if (!proxyConfiguration && !usedFallbackProxy) {
                    const fallbackProxy = await getFallbackProxyConfiguration();
                    if (fallbackProxy) {
                        usedFallbackProxy = true;
                        ({ body, statusCode } = await fetchPage(pageUrl, fallbackProxy));
                    }
                }

                if (isLikelyBlocked(body)) {
                    log.warning(`Potential anti-bot block detected at page ${page}. Switching to Playwright(Firefox) fallback for this product.`);
                    await Actor.setValue('debug-blocked-page.html', body, { contentType: 'text/html' });
                    shouldUsePlaywrightForProduct = true;
                    break;
                }
            }

            let initialState = body ? extractStructuredStateFromHtml(body) : null;
            if (!initialState) {
                if (!proxyConfiguration && !usedFallbackProxy) {
                    const fallbackProxy = await getFallbackProxyConfiguration();
                    if (fallbackProxy) {
                        usedFallbackProxy = true;
                        ({ body, statusCode } = await fetchPage(pageUrl, fallbackProxy));
                    }
                }

                initialState = body ? extractStructuredStateFromHtml(body) : null;
            }

            if (!initialState) {
                log.warning(`No structured state found at page ${page}. Switching to Playwright(Firefox) fallback for this product.`);
                if (body) await Actor.setValue(`debug-missing-state-page-${page}.html`, body, { contentType: 'text/html' });
                shouldUsePlaywrightForProduct = true;
                break;
            }

            if (page === 1) {
                const discoveredCanonical = extractCanonicalReviewUrl(initialState, canonicalReviewUrl);
                if (discoveredCanonical !== canonicalReviewUrl) {
                    canonicalReviewUrl = discoveredCanonical;
                    log.debug(`Using canonical URL: ${canonicalReviewUrl}`);
                }
            }

            let extractedReviews = extractReviewsFromState(initialState, pageUrl);

            log.debug(`Page ${page}: extracted ${extractedReviews.length} reviews from structured state.`);

            if (!extractedReviews.length) {
                if (page === 1 && body) {
                    await Actor.setValue('debug-empty-reviews-page-1.html', body, { contentType: 'text/html' });
                }
                if (page === 1) {
                    log.warning('Page 1: extracted 0 reviews from structured state. Switching to Playwright(Firefox) fallback for this product.');
                    shouldUsePlaywrightForProduct = true;
                }
                break;
            }

            let newCount = 0;
            for (const review of extractedReviews) {
                if (totalSaved >= resultsWanted) break;
                if (seenReviewIds.has(review.review_id)) continue;

                seenReviewIds.add(review.review_id);
                batchBuffer.push(review);
                totalSaved++;
                newCount++;
            }

            await flushBatch();

            if (newCount === 0) noNewDataPages++;
            else noNewDataPages = 0;

            if (noNewDataPages >= 2) {
                log.debug(`Stopping pagination after repeated duplicate pages at: ${canonicalReviewUrl}`);
                break;
            }

            if (extractedReviews.length < PAGE_SIZE_ESTIMATE) break;

            await sleep(750 + Math.floor(Math.random() * 600));
        }

        if (shouldUsePlaywrightForProduct && totalSaved < resultsWanted && !isNearTimeout()) {
            const remaining = resultsWanted - totalSaved;
            const pagesToTry = Math.min(
                maxPagesPerProduct,
                Math.max(2, Math.ceil(remaining / PAGE_SIZE_ESTIMATE) + 3),
                15,
            );

            log.debug(`Running Playwright(Firefox) fallback for: ${canonicalReviewUrl} (pages=${pagesToTry})`);

            let pushChain = Promise.resolve();
            await runPlaywrightFirefoxFallback({
                reviewBaseUrl: canonicalReviewUrl,
                pagesToTry,
                resultsWanted,
                proxyConfiguration,
                seenReviewIds,
                isNearTimeout,
                shouldStop: () => totalSaved >= resultsWanted,
                pushReview: async (review) => {
                    pushChain = pushChain.then(async () => {
                        if (totalSaved >= resultsWanted) return;
                        batchBuffer.push(review);
                        totalSaved++;
                        await flushBatch();
                    });
                    await pushChain;
                },
            });
        }
    }

    await flushBatch(true);
    log.info(`Done. Saved ${totalSaved} reviews.`);

    if (totalSaved === 0) {
        const message = 'No reviews were extracted. The page may be blocked, the product has no reviews, or input URL needs pid/lid query params.';
        if (failOnEmpty) {
            throw new Error(message);
        }
        log.warning(message);
        await Actor.setValue('EMPTY_RESULT_INFO', {
            message,
            suggestion: 'Try a full Flipkart product URL that includes pid/lid query params, and use residential proxy.',
            startUrls,
        });
    }
}

try {
    await main();
    await Actor.exit();
} catch (error) {
    log.exception(error, 'Actor failed');
    await Actor.fail(`Actor failed: ${error.message}`);
}
