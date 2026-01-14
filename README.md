# Flipkart Reviews Scraper

Extract authentic customer reviews from Flipkart products with ease. This powerful tool collects product ratings, review text, author information, dates, and verified purchase status – everything you need for market research, sentiment analysis, or product insights.

## What does this scraper do?

This scraper retrieves customer reviews from any Flipkart product page. Simply provide the product review URL, and the scraper will automatically:

- Extract all review details including ratings, titles, and full review text
- Identify verified purchases (Certified Buyer status)
- Capture reviewer names and review dates
- Handle pagination automatically to collect the desired number of reviews
- Deduplicate reviews to ensure clean, unique data
- Save all data in a structured format ready for analysis

## Why scrape Flipkart reviews?

Flipkart reviews contain valuable insights for:

- **Product Research** – Understand what customers love or dislike about specific products
- **Competitive Analysis** – Monitor competitor products and customer sentiment
- **Market Intelligence** – Identify trends, common complaints, and feature requests
- **Sentiment Analysis** – Analyze customer satisfaction at scale
- **E-commerce Strategy** – Make data-driven decisions about product listings and pricing

## Features

✅ **Fast & Efficient** – Optimized for speed using advanced API and HTML extraction\
✅ **Reliable** – Handles errors gracefully with automatic retries\
✅ **Accurate** – Extracts complete review data including ratings, text, authors, and dates\
✅ **Scalable** – Process single products or multiple products in one run\
✅ **Clean Data** – Automatic deduplication ensures no duplicate reviews\
✅ **Pagination** – Automatically navigates through all review pages

## Input Configuration

The scraper accepts the following input parameters:

### Required

- **Product Review URL** (`startUrl`) – The Flipkart product review page URL
  - Example: `https://www.flipkart.com/product-name/product-reviews/itmXXXXXXXXXX`

### Optional

- **Maximum Reviews** (`results_wanted`) – Maximum number of reviews to collect (default: 20)
- **Maximum Pages** (`max_pages`) – Safety limit on review pages to visit (default: 20)
- **Multiple URLs** (`startUrls`) – Array of review URLs to scrape multiple products
- **Proxy Configuration** (`proxyConfiguration`) – Custom proxy settings (residential proxies recommended)

### Example Input

```json
{
  "startUrl": "https://www.flipkart.com/adidas-ampligy-m-running-shoes-men/product-reviews/itmab79cd4ce225d",
  "results_wanted": 50,
  "max_pages": 10
}
```

## Output Format

Each review is saved with the following structured data:

| Field | Type | Description |
|-------|------|-------------|
| `product_name` | String | Name of the product being reviewed |
| `product_id` | String | Unique Flipkart product identifier |
| `review_id` | String | Unique review identifier |
| `rating` | Number | Star rating (1-5) |
| `title` | String | Review headline/title |
| `review_text` | String | Full review content |
| `author` | String | Name of the reviewer |
| `date` | String | Review submission date |
| `verified_purchase` | Boolean | Whether the reviewer is a certified buyer |
| `helpful_count` | Number | Number of helpful votes |
| `review_images` | Array | URLs of review images (if any) |
| `url` | String | Source URL of the review |

### Example Output

```json
{
  "product_name": "adidas ampligy m running shoes men",
  "product_id": "itmab79cd4ce225d",
  "review_id": "abc123xyz",
  "rating": 5,
  "title": "Excellent product!",
  "review_text": "Very comfortable shoes, great for running. Highly recommended!",
  "author": "John Doe",
  "date": "Apr, 2023",
  "verified_purchase": true,
  "helpful_count": 15,
  "review_images": [],
  "url": "https://www.flipkart.com/adidas-ampligy-m-running-shoes-men/product-reviews/itmab79cd4ce225d?page=1"
}
```

## How to Use

### On Apify Platform

1. Navigate to the [Flipkart Reviews Scraper](https://apify.com/your-username/flipkart-reviews-scraper) on Apify
2. Click **Try for free**
3. Enter the Flipkart product review URL in the **Product Review URL** field
4. Configure **Maximum Reviews** and other settings as needed
5. Click **Start** to begin scraping
6. Download your data in JSON, CSV, Excel, or other formats

### Locally via Apify CLI

```bash
apify run
```

Ensure you have configured your input in `INPUT.json` before running.

## Use Cases

### Market Research
Analyze customer feedback to understand product strengths and weaknesses across different categories.

### Competitor Monitoring
Track competitor product reviews to identify market gaps and opportunities.

### Product Development
Gather customer pain points and feature requests to inform product improvements.

### Sentiment Analysis
Build datasets for machine learning models to classify customer sentiment.

### Quality Assurance
Monitor your own products for recurring issues or quality concerns.

## Performance & Cost

- **Speed**: Scrapes 20 reviews in approximately 30-60 seconds
- **Cost**: Minimal – optimized for efficient resource usage
- **Proxies**: Residential proxies recommended for best reliability

## Important Notes

⚠️ **Respect Rate Limits** – Use reasonable `results_wanted` values to avoid overloading Flipkart's servers\
⚠️ **Use Proxies** – Residential proxies are recommended for consistent scraping\
⚠️ **Review URL Format** – Ensure you're using the product review page URL, not the product page URL

## Troubleshooting

### No results returned
- Verify the URL is a Flipkart **product review** page (should contain `/product-reviews/` in the URL)
- Check that the product has reviews available
- Ensure proxy configuration is correct

### Scraper times out
- Reduce `results_wanted` to a smaller number
- Decrease `max_pages` to limit pagination
- Verify your proxy configuration is working

### Missing data fields
- Some reviews may not have all fields (e.g., no title, no images)
- Older reviews might have different data structures

## Data Privacy & Ethics

This scraper is designed to collect publicly available data from Flipkart. Please ensure your use case complies with:

- Flipkart's Terms of Service
- Applicable data protection regulations (GDPR, CCPA, etc.)
- Ethical web scraping practices

## Support

Need help or have questions?

- 📧 Contact: [your-email@example.com]
- 🐛 Report issues on GitHub
- 💬 Join our community forum

## About

This scraper is maintained by Shahid Irfan. It's optimized for reliability, speed, and data accuracy to help businesses make informed decisions based on customer feedback.

---

**Ready to extract valuable insights from Flipkart reviews? Start scraping now!**