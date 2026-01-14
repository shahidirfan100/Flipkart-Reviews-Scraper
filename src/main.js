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

                if (path.includes('/product-reviews/')) return inputUrl;

                const productMatch = path.match(/\/([^\/]+)\/p\/(itm[a-z0-9]+)/i);
                if (productMatch) {
                    return `https://www.flipkart.com/${productMatch[1]}/product-reviews/${productMatch[2]}`;
                }

                const itmMatch = path.match(/(itm[a-z0-9]+)/i);
                if (itmMatch) {
                    const pathParts = path.split('/').filter(p => p);
                    const productName = pathParts[0] || 'product';
                    return `https://www.flipkart.com/${productName}/product-reviews/${itmMatch[1]}`;
                }

                return inputUrl;
            } catch (e) {
                return inputUrl;
            }
        }

        const reviewUrls = initial.map(toReviewUrl);
        log.info(`Processing: ${reviewUrls[0]}`);

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

        let saved = 0;
        const seenReviewIds = new Set();
        let batchBuffer = [];

        function extractProductInfo(reviewUrl) {
            let match = reviewUrl.match(/\/([^\/]+)\/product-reviews\/(itm[a-z0-9]+)/i);
            if (!match) match = reviewUrl.match(/\/([^\/]+)\/p\/(itm[a-z0-9]+)/i);
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

            // Find all review row containers - these are the main review blocks
            // Updated selectors based on latest investigation
            let reviewContainers = $('div.col.EPCNoQ');

            // Fallback selectors
            if (reviewContainers.length === 0) {
                reviewContainers = $('div.gMdEY7.rmo75L');
            }
            if (reviewContainers.length === 0) {
                // Try to find divs that contain rating elements
                reviewContainers = $('div.EkgbOr, div.XQDdHH, div._3LWZlK, div.MKiFS6').closest('div.col, div.row').parent();
            }

            reviewContainers.each((_, container) => {
                try {
                    const $c = $(container);

                    // Rating - updated selectors: EkgbOr is the new class
                    let rating = null;
                    const ratingElem = $c.find('div.EkgbOr, div.XQDdHH, div._3LWZlK, div.MKiFS6').first();
                    if (ratingElem.length) {
                        const ratingMatch = ratingElem.text().trim().match(/(\d+)/);
                        if (ratingMatch) rating = parseInt(ratingMatch[1]);
                    }

                    // Title - qW2QI1 is the new class
                    let title = null;
                    const titleElem = $c.find('p.qW2QI1, p.z9E0IG, p._2-N1Vz').first();
                    if (titleElem.length) {
                        title = titleElem.text().trim() || null;
                    }

                    // Review text - ZmyHeo, t-ZTKy, HM2vKw, kXosBy (parent of READ MORE)
                    let reviewText = null;
                    const textElem = $c.find('div.ZmyHeo, div.t-ZTKy, div.HM2vKw, div._6K-7Co').first();
                    if (textElem.length) {
                        reviewText = textElem.text().replace(/READ MORE/gi, '').trim() || null;
                    }

                    // Skip if no rating or no text
                    if (!rating || (!reviewText && !title)) return;

                    // If no title, use first sentence of review
                    if (!title && reviewText) {
                        const firstSentence = reviewText.split('.')[0];
                        if (firstSentence && firstSentence.length < 100) title = firstSentence;
                    }

                    // Author - zJ1ZGa ZDi3w2, _2sc7Ds
                    const author = $c.find('p.zJ1ZGa.ZDi3w2, p._2sc7Ds').first().text().trim() || null;

                    // Date - zJ1ZGa (without ZDi3w2)
                    let date = null;
                    $c.find('p.zJ1ZGa, p._2sc7Ds').each((_, elem) => {
                        const text = $(elem).text().trim();
                        // Match patterns like "Apr, 2024" or "8 months ago"
                        if (text.match(/\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec),?\s*\d{4}/i) ||
                            text.match(/\d+\s*(days?|months?|years?)\s*ago/i)) {
                            date = text;
                            return false;
                        }
                    });

                    const verified = $c.find('p.Zhmv6U').length > 0 || $c.text().includes('Certified Buyer');

                    let helpfulCount = 0;
                    const helpfulMatch = $c.find('span.Fp3hrV, span._3c3Px5').first().text().trim().match(/(\d+)/);
                    if (helpfulMatch) helpfulCount = parseInt(helpfulMatch[1]);

                    const reviewId = Buffer.from(`${author}_${title}_${reviewText}`.slice(0, 100)).toString('base64').slice(0, 20);

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
