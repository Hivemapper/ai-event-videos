import type { NextConfig } from "next";
import { resolve } from "path";

const nextConfig: NextConfig = {
  serverExternalPackages: ["better-sqlite3"],
  experimental: {
    optimizePackageImports: ["lucide-react", "radix-ui"],
  },
  turbopack: {
    root: resolve(__dirname),
  },
};

export default nextConfig;
