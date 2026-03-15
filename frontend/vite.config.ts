import { sveltekit } from "@sveltejs/kit/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    proxy: {
      "/tasks": "http://localhost:4300",
      "/feed": "http://localhost:4300",
      "/health": "http://localhost:4300",
    },
  },
});
