import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const gateway = process.env.GATEWAY_URL ?? 'http://localhost:3002';

// Прокси /ws → gateway: фронт ходит на свой же origin (как будет на проде за reverse proxy),
// без CORS и с одинаковым URL в dev и prod.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: { '/ws': { target: gateway, ws: true } },
  },
});
