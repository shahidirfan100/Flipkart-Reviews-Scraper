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
            throw new Error('No start URL provided. Please provide a Flipkart product review URL.');
        }

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

        let saved = 0;
        const seenReviewIds = new Set();
        let batchBuffer = [];

        function extractProductInfo(reviewUrl) {
            const match = reviewUrl.match(/\/([^\/]+)\/product-reviews\/(itm[a-z0-9]+)/i);
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

            $('div.gMdEY7.rmo75L').each((_, container) => {
                try {
                    const $c = $(container);

                    let rating = null;
                    const ratingMatch = $c.find('div.MKiFS6').first().text().trim().match(/(\d+)/);
                    if (ratingMatch) rating = parseInt(ratingMatch[1]);

                    const reviewText = $c.find('div.HM2vKw').first().text().trim() || null;
                    if (!rating || !reviewText) return;

                    let title = null;
                    const firstSentence = reviewText.split('.')[0];
                    if (firstSentence && firstSentence.length < 100) title = firstSentence;

                    const author = $c.find('p.zJ1ZGa.ZDi3w2').first().text().trim() || null;

                    let date = null;
                    $c.find('p.zJ1ZGa').each((_, elem) => {
                        if (!$(elem).hasClass('ZDi3w2')) {
                            date = $(elem).text().trim();
                            return false;
                        }
                    });

                    const verified = $c.find('p.Zhmv6U').length > 0;

                    let helpfulCount = 0;
                    const helpfulMatch = $c.find('span.Fp3hrV').first().text().trim().match(/(\d+)/);
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
                sessionOptions: {
                    maxUsageCount: 10,
                },
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
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache',
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
        await crawler.run(initial.map(u => ({ url: u, userData: { pageNo: 1, baseUrl: u } })));
        await pushBatch(true);
        log.info(`✅ Done. Saved ${saved} reviews.`);
    } finally {
        await Actor.exit();
    }
}

main().catch(err => { console.error(err); process.exit(1); });
