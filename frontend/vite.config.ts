import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const environment = loadEnv(mode, "..", "");
  return {
    server: {
      port: 5173,
      proxy: {
        "/api": {
          target: "http://127.0.0.1:8000",
          changeOrigin: true,
          headers: {
            "X-Analysis-Token": environment.ANALYSIS_API_TOKEN ?? "local-development-only",
          },
        },
      },
    },
  };
});
