import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  // Proxy API and dashboard requests to the multiadmin backend so the browser
  // only needs to reach the Next.js server (no build-time URL baking needed).
  async rewrites() {
    const destination =
      process.env.MULTIADMIN_API_URL || "http://localhost:15000";
    return [
      {
        source: "/api/v1/:path*",
        destination: `${destination}/api/v1/:path*`,
      },
      {
        source: "/proxy/:path*",
        destination: `${destination}/proxy/:path*`,
      },
    ];
  },
};

export default nextConfig;
