// Flipkart Reviews Scraper - Fast HTTP-based implementation
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { gotScraping } from 'got-scraping';

// Single-entrypoint main
await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            results_wanted: RESULTS_WANTED_RAW = 20,
            max_pages: MAX_PAGES_RAW = 20,
            startUrl,
            startUrls,
            url,
            proxyConfiguration,
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 20;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 20;

        // Build initial URLs
        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) initial.push(...startUrls);
        if (startUrl) initial.push(startUrl);
        if (url) initial.push(url);

        if (!initial.length) {
            throw new Error('No start URL provided. Please provide a Flipkart product review URL.');
        }

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

        let saved = 0;
        const seenReviewIds = new Set();
        let batchBuffer = [];

        // Extract product ID and name from URL
        function extractProductInfo(reviewUrl) {
            // URL format: https://www.flipkart.com/product-name/product-reviews/itmXXXXXXXXXXXX
            const match = reviewUrl.match(/\/([^\/]+)\/product-reviews\/(itm[a-z0-9]+)/i);
            if (!match) return null;
            return {
                productName: match[1].replace(/-/g, ' '),
                productId: match[2],
            };
        }

        // Try to fetch reviews via JSON API first
        async function fetchViaAPI(reviewUrl, pageNum, proxyUrl) {
            try {
                const productInfo = extractProductInfo(reviewUrl);
                if (!productInfo) return null;

                const urlObj = new URL(reviewUrl);
                const pageUri = `${urlObj.pathname}?page=${pageNum}`;

                const apiUrl = 'https://1.rome.api.flipkart.com/api/4/page/fetch';

                const response = await gotScraping({
                    url: apiUrl,
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'X-User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 FKUA/website/42/website/Desktop',
                        'Referer': reviewUrl,
                        'Accept': 'application/json',
                        'Origin': 'https://www.flipkart.com',
                    },
                    body: JSON.stringify({
                        pageUri: pageUri,
                        pageContext: { fetchSeoData: true },
                    }),
                    proxyUrl,
                    responseType: 'json',
                    timeout: { request: 30000 },
                });

                if (response.body && typeof response.body === 'object') {
                    log.info(`API fetch successful for page ${pageNum}`);
                    return { data: response.body, productInfo };
                }
                return null;
            } catch (err) {
                log.warning(`API fetch failed for page ${pageNum}: ${err.message}`);
                return null;
            }
        }

        // Extract reviews from API response
        function extractReviewsFromAPI(apiData, productInfo) {
            const reviews = [];
            try {
                // Correct path: RESPONSE.pageData.slots[].widget where type === 'REVIEWS'
                const slots = apiData?.RESPONSE?.pageData?.slots;
                if (!Array.isArray(slots)) {
                    log.warning('API response missing slots array');
                    return reviews;
                }

                // Find slots with review data
                for (const slot of slots) {
                    if (!slot?.widget) continue;

                    // Look for REVIEWS widget
                    if (slot.widget.type === 'REVIEWS' && slot.widget.data?.renderableComponents) {
                        const components = slot.widget.data.renderableComponents;

                        for (const component of components) {
                            const reviewData = component?.value;
                            if (!reviewData) continue;

                            // Extract review fields
                            const reviewId = reviewData.id ||
                                Buffer.from(`${reviewData.author}_${reviewData.created}`.slice(0, 50)).toString('base64').slice(0, 20);

                            reviews.push({
                                product_name: productInfo.productName,
                                product_id: productInfo.productId,
                                review_id: reviewId,
                                rating: reviewData.rating || null,
                                title: reviewData.title || null,
                                review_text: reviewData.text || null,
                                author: reviewData.author || null,
                                date: reviewData.created || null,
                                verified_purchase: reviewData.certifiedBuyer === true,
                                helpful_count: reviewData.upvote?.value?.count || 0,
                                review_images: [],
                                url: null, // Will be added later
                            });
                        }
                    }
                }

                if (reviews.length === 0) {
                    log.warning('No reviews found in API response slots');
                }
            } catch (err) {
                log.error(`Error extracting reviews from API: ${err.message}`);
            }
            return reviews;
        }

        // Extract reviews from HTML
        function extractReviewsFromHTML($, reviewUrl) {
            const reviews = [];
            const productInfo = extractProductInfo(reviewUrl);
            if (!productInfo) return reviews;

            // Use correct CSS selectors: div.gMdEY7.rmo75L
            const reviewContainers = $('div.gMdEY7.rmo75L');
            log.info(`Found ${reviewContainers.length} review containers in HTML`);

            reviewContainers.each((index, container) => {
                try {
                    const $container = $(container);

                    // Rating: div.MKiFS6
                    let rating = null;
                    const ratingElem = $container.find('div.MKiFS6').first();
                    if (ratingElem.length) {
                        const ratingText = ratingElem.text().trim();
                        const match = ratingText.match(/(\d+)/);
                        if (match) rating = parseInt(match[1]);
                    }

                    // Review text: div.HM2vKw
                    const reviewTextElem = $container.find('div.HM2vKw').first();
                    const reviewText = reviewTextElem.text().trim() || null;

                    // Title: use first sentence as title
                    let title = null;
                    if (reviewText) {
                        const firstSentence = reviewText.split('.')[0];
                        if (firstSentence && firstSentence.length < 100) {
                            title = firstSentence;
                        }
                    }

                    // Author: p.zJ1ZGa.ZDi3w2
                    const authorElem = $container.find('p.zJ1ZGa.ZDi3w2').first();
                    const author = authorElem.text().trim() || null;

                    // Date: p.zJ1ZGa (without ZDi3w2)
                    let date = null;
                    const dateElems = $container.find('p.zJ1ZGa');
                    dateElems.each((_, elem) => {
                        if (!$(elem).hasClass('ZDi3w2')) {
                            date = $(elem).text().trim();
                            return false;
                        }
                    });

                    // Verified: p.Zhmv6U
                    const verifiedElem = $container.find('p.Zhmv6U');
                    const verified = verifiedElem.length > 0 || $container.text().includes('Certified Buyer');

                    // Helpful count: span.Fp3hrV
                    let helpfulCount = 0;
                    const helpfulElem = $container.find('span.Fp3hrV').first();
                    if (helpfulElem.length) {
                        const countText = helpfulElem.text().trim();
                        const match = countText.match(/(\d+)/);
                        if (match) helpfulCount = parseInt(match[1]);
                    }

                    const reviewId = Buffer.from(`${author}_${reviewText}`.slice(0, 100)).toString('base64').slice(0, 20);

                    if (rating && reviewText) {
                        reviews.push({
                            product_name: productInfo.productName,
                            product_id: productInfo.productId,
                            review_id: reviewId,
                            rating,
                            title,
                            review_text: reviewText,
                            author,
                            date,
                            verified_purchase: verified,
                            helpful_count: helpfulCount,
                            review_images: [],
                        });
                    }
                } catch (err) {
                    log.warning(`Failed to parse review ${index}: ${err.message}`);
                }
            });

            return reviews;
        }

        // Batch push helper
        async function pushBatch(force = false) {
            if (batchBuffer.length >= 10 || (force && batchBuffer.length > 0)) {
                await Dataset.pushData(batchBuffer);
                log.info(`✓ Saved ${batchBuffer.length} reviews (Total: ${saved})`);
                batchBuffer = [];
            }
        }

        // Build pagination URLs
        function buildPaginationUrl(baseUrl, pageNum) {
            const urlObj = new URL(baseUrl);
            urlObj.searchParams.set('page', pageNum);
            return urlObj.href;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 3,
            useSessionPool: true,
            maxConcurrency: 5,
            requestHandlerTimeoutSecs: 90,
            async requestHandler({ request, $, log: crawlerLog, proxyInfo }) {
                const pageNo = request.userData?.pageNo || 1;
                const baseUrl = request.userData?.baseUrl || request.url;

                crawlerLog.info(`Processing page ${pageNo}: ${request.url}`);

                let reviews = [];
                let apiSuccess = false;

                // Try API first
                const proxyUrl = proxyInfo?.url;
                const apiResult = await fetchViaAPI(baseUrl, pageNo, proxyUrl);

                if (apiResult && apiResult.data) {
                    reviews = extractReviewsFromAPI(apiResult.data, apiResult.productInfo);
                    if (reviews.length > 0) {
                        apiSuccess = true;
                        crawlerLog.info(`✓ API: Extracted ${reviews.length} reviews from page ${pageNo}`);
                    }
                }

                // Fallback to HTML parsing
                if (!apiSuccess || reviews.length === 0) {
                    crawlerLog.info(`Falling back to HTML parsing for page ${pageNo}`);
                    reviews = extractReviewsFromHTML($, baseUrl);
                    crawlerLog.info(`✓ HTML: Extracted ${reviews.length} reviews from page ${pageNo}`);
                }

                // Process and save reviews
                for (const review of reviews) {
                    if (saved >= RESULTS_WANTED) break;

                    // Deduplicate by review ID
                    if (seenReviewIds.has(review.review_id)) continue;
                    seenReviewIds.add(review.review_id);

                    // Add URL
                    review.url = request.url;

                    batchBuffer.push(review);
                    saved++;

                    // Push batch if buffer is full
                    await pushBatch();
                }

                // Check if we need more pages
                if (saved < RESULTS_WANTED && pageNo < MAX_PAGES && reviews.length > 0) {
                    const nextPageUrl = buildPaginationUrl(baseUrl, pageNo + 1);
                    crawlerLog.info(`Queueing page ${pageNo + 1}`);
                    await crawler.addRequests([{
                        url: nextPageUrl,
                        userData: { pageNo: pageNo + 1, baseUrl },
                    }]);
                } else {
                    crawlerLog.info(`Stopping pagination: saved=${saved}, wanted=${RESULTS_WANTED}, pageNo=${pageNo}, maxPages=${MAX_PAGES}`);
                }
            },
            async failedRequestHandler({ request }, error) {
                log.error(`Request ${request.url} failed: ${error.message}`);
            },
        });

        // Start crawling
        await crawler.run(initial.map(u => ({ url: u, userData: { pageNo: 1, baseUrl: u } })));

        // Push remaining batch
        await pushBatch(true);

        log.info(`✅ Scraping completed. Total reviews saved: ${saved}`);
    } finally {
        await Actor.exit();
    }
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
