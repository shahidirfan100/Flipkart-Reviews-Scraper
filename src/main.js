// Flipkart Reviews Scraper - Fast & Stealthy HTTP-based HTML parsing
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            results_wanted: RESULTS_WANTED_RAW = 20,
            startUrl,
            startUrls,
            url,
            proxyConfiguration,
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 20;

        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) initial.push(...startUrls);
        if (startUrl) initial.push(startUrl);
        if (url) initial.push(url);

        if (!initial.length) {
            throw new Error('No start URL provided. Please provide a Flipkart product or review URL.');
        }

        // Convert product URL to review URL if needed
        function toReviewUrl(inputUrl) {
            try {
                const urlObj = new URL(inputUrl);
                const path = urlObj.pathname;

                // Already a review URL
                if (path.includes('/product-reviews/')) {
                    return inputUrl;
                }

                // Product URL format: /product-name/p/itmXXX
                const productMatch = path.match(/\/([^\/]+)\/p\/(itm[a-z0-9]+)/i);
                if (productMatch) {
                    const productName = productMatch[1];
                    const productId = productMatch[2];
                    return `https://www.flipkart.com/${productName}/product-reviews/${productId}`;
                }

                // If URL has pid parameter, extract it
                const pid = urlObj.searchParams.get('pid');
                if (pid) {
                    const pathParts = path.split('/').filter(p => p);
                    const productName = pathParts[0] || 'product';
                    // Find itm ID in the path
                    const itmMatch = path.match(/(itm[a-z0-9]+)/i);
                    const productId = itmMatch ? itmMatch[1] : pid;
                    return `https://www.flipkart.com/${productName}/product-reviews/${productId}`;
                }

                return inputUrl;
            } catch (e) {
                return inputUrl;
            }
        }

        const reviewUrls = initial.map(toReviewUrl);
        log.info(`Processing ${reviewUrls.length} URL(s): ${reviewUrls[0]}`);

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

        let saved = 0;
        const seenReviewIds = new Set();
        let batchBuffer = [];

        function extractProductInfo(reviewUrl) {
            // Handle both /product-reviews/itmXXX and /p/itmXXX formats
            let match = reviewUrl.match(/\/([^\/]+)\/product-reviews\/(itm[a-z0-9]+)/i);
            if (!match) {
                match = reviewUrl.match(/\/([^\/]+)\/p\/(itm[a-z0-9]+)/i);
            }
            if (!match) return null;
            return {
                productName: match[1].replace(/-/g, ' '),
                productId: match[2],
            };
        }

        function extractReviewsFromHTML($, reviewUrl) {
            const reviews = [];
            const productInfo = extractProductInfo(reviewUrl);
            if (!productInfo) return reviews;

            // Try multiple possible review container selectors
            let reviewContainers = $('div.gMdEY7.rmo75L');

            // Fallback selectors if primary doesn't work
            if (reviewContainers.length === 0) {
                reviewContainers = $('div.col.EPCmJX');
            }
            if (reviewContainers.length === 0) {
                reviewContainers = $('div[data-id]').filter((_, el) => {
                    return $(el).find('div._3LWZlK, div.XQDdHH').length > 0;
                });
            }

            reviewContainers.each((_, container) => {
                try {
                    const $c = $(container);

                    // Rating - try multiple selectors
                    let rating = null;
                    let ratingElem = $c.find('div.MKiFS6, div._3LWZlK, div.XQDdHH').first();
                    if (ratingElem.length) {
                        const ratingMatch = ratingElem.text().trim().match(/(\d+)/);
                        if (ratingMatch) rating = parseInt(ratingMatch[1]);
                    }

                    // Review text - try multiple selectors
                    let reviewText = null;
                    let reviewTextElem = $c.find('div.HM2vKw, div.t-ZTKy, div.ZmyHeo, div._6K-7Co').first();
                    if (reviewTextElem.length) {
                        reviewText = reviewTextElem.text().trim() || null;
                    }

                    if (!rating || !reviewText) return;

                    let title = null;
                    const firstSentence = reviewText.split('.')[0];
                    if (firstSentence && firstSentence.length < 100) title = firstSentence;

                    // Author - try multiple selectors
                    const author = $c.find('p.zJ1ZGa.ZDi3w2, p._2sc7Ds, span._2V4MzO').first().text().trim() || null;

                    // Date
                    let date = null;
                    $c.find('p.zJ1ZGa, p._2sc7Ds').each((_, elem) => {
                        const text = $(elem).text().trim();
                        if (text.match(/\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec),?\s*\d{4}/i)) {
                            date = text;
                            return false;
                        }
                    });

                    const verified = $c.find('p.Zhmv6U').length > 0 || $c.text().includes('Certified Buyer');

                    let helpfulCount = 0;
                    const helpfulMatch = $c.find('span.Fp3hrV, span._3c3Px5').first().text().trim().match(/(\d+)/);
                    if (helpfulMatch) helpfulCount = parseInt(helpfulMatch[1]);

                    const reviewId = Buffer.from(`${author}_${reviewText}`.slice(0, 100)).toString('base64').slice(0, 20);

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
                } catch (err) { /* skip */ }
            });

            return reviews;
        }

        async function pushBatch(force = false) {
            if (batchBuffer.length >= 10 || (force && batchBuffer.length > 0)) {
                await Dataset.pushData(batchBuffer);
                batchBuffer = [];
            }
        }

        function buildPaginationUrl(baseUrl, pageNum) {
            const urlObj = new URL(baseUrl);
            urlObj.searchParams.set('page', pageNum);
            return urlObj.href;
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 3,
            useSessionPool: true,
            persistCookiesPerSession: true,
            sessionPoolOptions: {
                maxPoolSize: 50,
                sessionOptions: { maxUsageCount: 10 },
            },
            maxConcurrency: 5,
            minConcurrency: 1,
            requestHandlerTimeoutSecs: 60,
            navigationTimeoutSecs: 45,
            preNavigationHooks: [
                async ({ request }) => {
                    request.headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                        'Sec-Ch-Ua-Mobile': '?0',
                        'Sec-Ch-Ua-Platform': '"Windows"',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                    };
                },
            ],
            async requestHandler({ request, $ }) {
                const pageNo = request.userData?.pageNo || 1;
                const baseUrl = request.userData?.baseUrl || request.url;

                const reviews = extractReviewsFromHTML($, baseUrl);

                for (const review of reviews) {
                    if (saved >= RESULTS_WANTED) break;
                    if (seenReviewIds.has(review.review_id)) continue;
                    seenReviewIds.add(review.review_id);

                    review.url = request.url;
                    batchBuffer.push(review);
                    saved++;
                    await pushBatch();
                }

                if (saved < RESULTS_WANTED && reviews.length > 0) {
                    await crawler.addRequests([{
                        url: buildPaginationUrl(baseUrl, pageNo + 1),
                        userData: { pageNo: pageNo + 1, baseUrl },
                    }]);
                }
            },
            async failedRequestHandler({ request }, error) {
                log.warning(`Failed: ${request.url}`);
            },
        });

        log.info(`Starting scrape for ${RESULTS_WANTED} reviews...`);
        await crawler.run(reviewUrls.map(u => ({ url: u, userData: { pageNo: 1, baseUrl: u } })));
        await pushBatch(true);
        log.info(`✅ Done. Saved ${saved} reviews.`);
    } finally {
        await Actor.exit();
    }
}

main().catch(err => { console.error(err); process.exit(1); });
