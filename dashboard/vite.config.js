import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

/**
 * Vite configuration for the LogFlow React dashboard.
 *
 * API Proxy
 * ---------
 * Requests from the browser to /api/* are proxied to the FastAPI server
 * (processing/api/main.py) at VITE_API_BASE_URL (default: http://localhost:8000).
 * This avoids CORS issues during local development and keeps client.js
 * endpoint paths clean (no need to repeat the base URL in every fetch call).
 *
 * The target is read from the environment at Vite startup time, so:
 *   - Copy .env.example → .env at the repo root.
 *   - Set VITE_API_BASE_URL=http://localhost:8000 (or Docker service URL).
 *
 * Port
 * ----
 * Dev server runs on port 5173 (Vite default), matching the docker-compose
 * placeholder service port mapping.
 */
export default defineConfig({
  plugins: [react()],

  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.VITE_API_BASE_URL || 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },

  define: {
    // Expose env vars to browser code (only VITE_ prefixed vars are safe)
    __API_BASE_URL__: JSON.stringify(
      process.env.VITE_API_BASE_URL || 'http://localhost:8000'
    ),
  },
})
