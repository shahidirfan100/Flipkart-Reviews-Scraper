FROM apify/actor-node-playwright-firefox:22-1.56.1

ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

# Check preinstalled packages
RUN npm ls crawlee apify puppeteer playwright || true

# Copy just package.json and package-lock.json first for caching
COPY --chown=myuser:myuser package*.json Dockerfile check-playwright-version.mjs ./

# Check Playwright version matches base image
RUN node check-playwright-version.mjs

RUN npm --quiet set progress=false \
	&& npm install --omit=dev --omit=optional \
	&& rm -r ~/.npm

COPY --chown=myuser:myuser . ./

ENV APIFY_LOG_LEVEL=INFO

CMD npm start --silent
