// config.js
import dotenv from 'dotenv';
dotenv.config();

export const ghostConfig = {
    url: process.env.GHOST_URL, // e.g., 'https://your-ghost-site.com'
    adminApiKey: process.env.GHOST_ADMIN_API_KEY // e.g., 'YOUR_ADMIN_API_KEY'
};
