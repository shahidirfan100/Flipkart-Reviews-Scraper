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
                        locationContext: {},
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
                // Navigate through the API response structure to find reviews
                // Flipkart API response is deeply nested
                const pageData = apiData?.RESPONSE?.pageData?.pageContext;
                if (!pageData) return reviews;

                // Try to find review widgets in the page data
                const reviewWidgets = [];
                
                // Helper to recursively search for review data
                function findReviews(obj) {
                    if (!obj || typeof obj !== 'object') return;
                    
                    for (const key in obj) {
                        const value = obj[key];
                        
                        // Look for review-like structures
                        if (value && typeof value === 'object') {
                            if (value.reviewId || value.review || value.rating) {
                                reviewWidgets.push(value);
                            }
                            findReviews(value);
                        }
                    }
                }
                
                findReviews(pageData);

                for (const reviewData of reviewWidgets) {
                    const reviewId = reviewData.reviewId || reviewData.id;
                    if (!reviewId) continue;

                    reviews.push({
                        product_name: productInfo.productName,
                        product_id: productInfo.productId,
                        review_id: reviewId,
                        rating: reviewData.rating || reviewData.overallRating || null,
                        title: reviewData.title || reviewData.reviewTitle || null,
                        review_text: reviewData.text || reviewData.reviewText || reviewData.review || null,
                        author: reviewData.author || reviewData.userName || reviewData.name || null,
                        date: reviewData.date || reviewData.reviewDate || reviewData.createdAt || null,
                        verified_purchase: reviewData.certifiedBuyer || reviewData.verified || false,
                        helpful_count: reviewData.helpfulCount || reviewData.helpfulVotes || 0,
                        review_images: Array.isArray(reviewData.images) ? reviewData.images : [],
                    });
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

            // Find all review containers
            const reviewContainers = $('div').filter((_, el) => {
                const text = $(el).text();
                return text.includes('Certified Buyer') || text.includes('★') || text.includes('READ MORE');
            });

            const uniqueContainers = new Set();
            
            reviewContainers.each((_, container) => {
                try {
                    const $container = $(container);
                    const containerHtml = $.html(container);
                    
                    // Avoid duplicates
                    if (uniqueContainers.has(containerHtml)) return;
                    uniqueContainers.add(containerHtml);

                    // Extract rating (look for star or numeric rating)
                    let rating = null;
                    const ratingElem = $container.find('div[class*="_3LWZlK"], div[class*="XQDdHH"]').first();
                    if (ratingElem.length) {
                        const ratingText = ratingElem.text().trim();
                        const match = ratingText.match(/(\d+)/);
                        if (match) rating = parseInt(match[1]);
                    }

                    // Extract title
                    const title = $container.find('p[class*="_2-N1Vz"], p[class*="z9E0IG"]').first().text().trim() || null;

                    // Extract review text
                    const reviewText = $container.find('div[class*="t-ZTKy"], div[class*="ZmyHeo"]').first().text().trim() || null;

                    // Extract author
                    const authorElem = $container.find('p[class*="_2sc7Ds"], p[class*="_2NsDsF"]').first();
                    const author = authorElem.text().trim() || null;

                    // Extract date (usually second p with same class or in metadata)
                    let date = null;
                    const dateElems = $container.find('p[class*="_2sc7Ds"], p[class*="_2NsDsF"]');
                    if (dateElems.length > 1) {
                        date = $(dateElems[1]).text().trim();
                    }

                    // Check for verified purchase
                    const verified = $container.text().includes('Certified Buyer');

                    // Generate review ID from content hash
                    const reviewId = Buffer.from(`${author}_${title}_${reviewText}`.slice(0, 100)).toString('base64').slice(0, 20);

                    // Only add if we have meaningful data
                    if ((title || reviewText) && rating) {
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
                            helpful_count: 0,
                            review_images: [],
                        });
                    }
                } catch (err) {
                    // Skip malformed reviews
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
