import express from 'express';
import bodyParser from 'body-parser';
import dotenv from 'dotenv';
import GhostAdminAPI from '@tryghost/admin-api';

dotenv.config();

const ghostConfig = {
    url: process.env.GHOST_URL,
    adminApiKey: process.env.GHOST_ADMIN_API_KEY
};

const app = express();
app.use(bodyParser.json());

const api = new GhostAdminAPI({
    url: ghostConfig.url,
    key: ghostConfig.adminApiKey,
    version: 'v5.0' // Updated to the new format
});

app.post('/create-post', async (req, res) => {
    try {
        console.log("Request received:", req.body); // Debug incoming request
        const { title, content } = req.body;
        console.log("mobiledoc", content);
        const newPost = await api.posts.add({
            title: title,
            mobiledoc: content,
            status: 'draft'
        });
        

        console.log(`Ghost post created: ${newPost.url}`);
        res.json({ success: true, post: newPost }); // Send newPost in the response
    } catch (error) {
        console.error("Error creating post:", error.message); // Log errors
        res.status(500).json({ success: false, error: error.message });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Ghost bridge service running on port ${PORT}`);
});
