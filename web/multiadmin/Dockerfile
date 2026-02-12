# Build stage - run on the host's native architecture to avoid slow QEMU emulation.
# Next.js standalone output is pure JS so it runs on any platform.
FROM --platform=$BUILDPLATFORM node:22-alpine AS builder

WORKDIR /app

# Install pnpm
RUN corepack enable && corepack prepare pnpm@9.15.5 --activate

# Copy package files
COPY package.json pnpm-lock.yaml ./

# Install dependencies
RUN pnpm install --frozen-lockfile

# Copy source
COPY . .

# Set the API URL for the build (browser will call this directly with CORS)
ARG NEXT_PUBLIC_MULTIADMIN_API_URL=http://localhost:18000
ENV NEXT_PUBLIC_MULTIADMIN_API_URL=${NEXT_PUBLIC_MULTIADMIN_API_URL}

# Build the application
RUN pnpm build

# Production stage
FROM node:22-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

# Create non-root user
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs

# Copy built application
COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs

EXPOSE 18100

ENV PORT=18100
ENV HOSTNAME="0.0.0.0"

CMD ["node", "server.js"]
