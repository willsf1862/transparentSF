import express from 'express';
import bodyParser from 'body-parser';
import dotenv from 'dotenv';
import GhostAdminAPI from '@tryghost/admin-api';
import path from 'path';
import tmp from 'tmp';
import * as fs from 'fs/promises';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import * as cheerio from 'cheerio';

// Define __filename and __dirname for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

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

/**
 * Uploads an image to Ghost and returns the image URL.
 *
 * @param {Buffer} imageBuffer - The image data as a buffer.
 * @param {string} imageName - The name to assign to the uploaded image.
 * @param {GhostAdminAPI} api - The initialized GhostAdminAPI instance.
 * @returns {string} - The URL of the uploaded image.
 */
const uploadImageToGhost = async (imageBuffer, imageName, api) => {
    const tempFile = tmp.fileSync({ postfix: path.extname(imageName) });
    const tempPath = tempFile.name;

    try {
        // Write the image buffer to the temporary file
        await fs.writeFile(tempPath, imageBuffer);
        console.log(`Image written to temporary path: ${tempPath}`);

        // Upload the image using the file path
        const response = await api.images.upload({
            file: tempPath,
            name: imageName
        });

        console.log(`Image successfully uploaded to Ghost: ${response.url}`);
        return response.url;
    } catch (error) {
        console.error('Error uploading image to Ghost:', error);
        throw error;
    } finally {
        tempFile.removeCallback();
        console.log(`Temporary image file deleted: ${tempPath}`);
    }
};

app.post('/create-post', async (req, res) => {
    try {
        console.log("Request received:", req.body); // Debug incoming request
        const { title, content } = req.body; // Destructure title and content

        if (!title || !content) {
            return res.status(400).json({ success: false, error: "Title and content are required." });
        }

        // Load the HTML content into cheerio
        const $ = cheerio.load(content);

        // Find all <img> tags
        const imgTags = $('img');

        console.log(`Found ${imgTags.length} image(s) in the content.`);

        // Iterate over each <img> tag
        for (let i = 0; i < imgTags.length; i++) {
            const img = imgTags[i];
            const src = $(img).attr('src');

            if (src) {
                console.log(`Processing image source: ${src}`);

                // Handle only local image paths (you may need to adjust this based on your use case)
                // For example, src might be "/images/photo.jpg" or an absolute path
                // Adjust the path resolution as necessary
                const imagePath = path.resolve(__dirname, '..', src); // Adjust the relative path as needed
                const imageName = path.basename(imagePath);

                try {
                    // Check if the file exists
                    await fs.access(imagePath);
                    console.log(`Image file exists at: ${imagePath}`);

                    // Read the image file
                    const imageBuffer = await fs.readFile(imagePath);

                    // Upload to Ghost and get the new URL
                    const imageUrl = await uploadImageToGhost(imageBuffer, imageName, api);
                    console.log(`Image URL from Ghost: ${imageUrl}`);

                    // Replace the src attribute with the new URL
                    $(img).attr('src', imageUrl);
                    console.log(`Replaced image src with Ghost URL: ${imageUrl}`);
                } catch (error) {
                    console.error(`Error processing image ${imagePath}:`, error);
                    // Optionally, you can decide to remove the image or leave it as is
                    // For now, we'll leave it unchanged and continue
                }
            }
        }

        // Get the updated HTML content
        const updatedContent = $.html();
        console.log("Updated content after processing images:", updatedContent);

        // Create the new post in Ghost
        const newPost = await api.posts.add({
            title: title,
            html: updatedContent, // Use updated HTML content
            status: 'draft'
        }, {
            source: 'html' // Specify source=html in query params
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
