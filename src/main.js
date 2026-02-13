import { Actor, log } from 'apify';
import { gotScraping } from 'got-scraping';

const FLIPKART_ORIGIN = 'https://www.flipkart.com';
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

function normalizeStartUrls(input) {
    const urls = [];
    const add = (value) => {
        if (typeof value === 'string' && value.trim()) urls.push(value.trim());
        else if (value && typeof value.url === 'string' && value.url.trim()) urls.push(value.url.trim());
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

        const productMatch = path.match(/\/([^/]+)\/p\/(itm[a-z0-9]+)/i);
        if (productMatch) {
            return `${FLIPKART_ORIGIN}/${productMatch[1]}/product-reviews/${productMatch[2]}`;
        }

        const itmMatch = path.match(/(itm[a-z0-9]+)/i);
        if (itmMatch) {
            const parts = path.split('/').filter(Boolean);
            const productSlug = parts[0] || 'product';
            return `${FLIPKART_ORIGIN}/${productSlug}/product-reviews/${itmMatch[1]}`;
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

    const isProxyTransportError = (error) => {
        const message = `${error?.message || ''} ${error?.cause?.message || ''}`.toLowerCase();
        return (
            message.includes('proxy responded with 595')
            || message.includes('econnreset')
            || message.includes('econnrefused')
            || message.includes('etimedout')
            || message.includes('socket hang up')
            || message.includes('proxy')
        );
    };

    const requestOnce = async (proxyUrl) => {
        const response = await gotScraping({
            url,
            proxyUrl,
            timeout: { request: 60000 },
            retry: { limit: 3 },
            headers: requestHeaders,
        });

        return {
            statusCode: response.statusCode,
            body: response.body,
        };
    };

    if (!proxyConfiguration) {
        return requestOnce(undefined);
    }

    try {
        const proxyUrl = await proxyConfiguration.newUrl();
        return await requestOnce(proxyUrl);
    } catch (error) {
        if (!isProxyTransportError(error)) throw error;

        log.warning(`Proxy request failed, retrying direct request for: ${url}`);
        return requestOnce(undefined);
    }
}

async function main() {
    const input = (await Actor.getInput()) || {};
    const resultsWanted = parseWantedCount(input.results_wanted, 20);
    const startUrls = normalizeStartUrls(input);

    if (!startUrls.length) {
        throw new Error('No start URL provided. Please provide a Flipkart product/review URL.');
    }

    const proxyConfiguration = input.proxyConfiguration
        ? await Actor.createProxyConfiguration({ ...input.proxyConfiguration })
        : undefined;

    let totalSaved = 0;
    const seenReviewIds = new Set();
    let batchBuffer = [];

    const flushBatch = async (force = false) => {
        if (batchBuffer.length >= 25 || (force && batchBuffer.length > 0)) {
            await Actor.pushData(batchBuffer);
            batchBuffer = [];
        }
    };

    for (const sourceUrl of startUrls) {
        if (totalSaved >= resultsWanted) break;

        let canonicalReviewUrl = toReviewUrl(sourceUrl);
        let noNewDataPages = 0;

        log.info(`Processing: ${canonicalReviewUrl}`);

        for (let page = 1; page <= MAX_PAGES_PER_PRODUCT && totalSaved < resultsWanted; page++) {
            const pageUrl = buildPagedUrl(canonicalReviewUrl, page);
            const { body, statusCode } = await fetchReviewPageHtml(pageUrl, proxyConfiguration);

            if (statusCode >= 400) {
                log.warning(`Skipping page ${page}, HTTP ${statusCode}: ${pageUrl}`);
                break;
            }

            if (isLikelyBlocked(body)) {
                log.warning(`Potential anti-bot block detected at page ${page}.`);
                await Actor.setValue('debug-blocked-page.html', body, { contentType: 'text/html' });
                break;
            }

            const initialState = extractInitialStateFromHtml(body);
            if (!initialState) {
                log.warning(`No __INITIAL_STATE__ found at page ${page}. Saving debug HTML.`);
                await Actor.setValue(`debug-missing-state-page-${page}.html`, body, { contentType: 'text/html' });
                break;
            }

            if (page === 1) {
                const discoveredCanonical = extractCanonicalReviewUrl(initialState, canonicalReviewUrl);
                if (discoveredCanonical !== canonicalReviewUrl) {
                    canonicalReviewUrl = discoveredCanonical;
                    log.info(`Using canonical URL: ${canonicalReviewUrl}`);
                }
            }

            const extractedReviews = extractReviewsFromState(initialState, pageUrl);
            log.info(`Page ${page}: extracted ${extractedReviews.length} reviews from structured state.`);

            if (!extractedReviews.length) break;

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
                log.info(`Stopping pagination after repeated duplicate pages at: ${canonicalReviewUrl}`);
                break;
            }

            if (extractedReviews.length < PAGE_SIZE_ESTIMATE) break;

            await sleep(750 + Math.floor(Math.random() * 600));
        }
    }

    await flushBatch(true);
    log.info(`Done. Saved ${totalSaved} reviews.`);
}

try {
    await main();
    await Actor.exit();
} catch (error) {
    log.exception(error, 'Actor failed');
    await Actor.fail(`Actor failed: ${error.message}`);
}
