import express from 'express';
import bodyParser from 'body-parser';
import dotenv from 'dotenv';
import GhostAdminAPI from '@tryghost/admin-api';
import path from 'path';
import tmp from 'tmp';
import * as fs from 'fs/promises';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import cheerio from 'cheerio'; // For HTML parsing
import { htmlToMobiledoc } from '@tryghost/html-to-mobiledoc'; // Hypothetical library

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

/**
 * Processes HTML content:
 * - Finds all <img> tags
 * - Uploads images to Ghost
 * - Replaces src attributes with Ghost URLs
 * - Converts HTML to Mobiledoc
 *
 * @param {string} html - The HTML content to process.
 * @param {GhostAdminAPI} api - The initialized GhostAdminAPI instance.
 * @returns {string} - The converted Mobiledoc JSON string.
 */
const processHtmlContent = async (html, api) => {
    const $ = cheerio.load(html);
    const imgElements = $('img');
    const uploadPromises = [];

    imgElements.each((index, img) => {
        const src = $(img).attr('src');
        if (src && !src.startsWith('http')) { // Assuming local paths
            const imagePath = path.resolve(__dirname, src);
            const imageName = path.basename(imagePath);
            console.log(`Processing image: ${imagePath}`);

            const uploadPromise = fs.access(imagePath)
                .then(() => fs.readFile(imagePath))
                .then(imageBuffer => uploadImageToGhost(imageBuffer, imageName, api))
                .then(imageUrl => {
                    $(img).attr('src', imageUrl);
                    console.log(`Replaced src for image ${imageName}: ${imageUrl}`);
                })
                .catch(error => {
                    console.error(`Error processing image ${imagePath}:`, error);
                    // Optionally, you can remove the image or keep the original src
                });

            uploadPromises.push(uploadPromise);
        }
    });

    // Wait for all image uploads to complete
    await Promise.all(uploadPromises);

    // Get the updated HTML
    const updatedHtml = $.html();
    console.log("Updated HTML:", updatedHtml);

    // Convert HTML to Mobiledoc
    let mobiledocObj;
    try {
        mobiledocObj = htmlToMobiledoc(updatedHtml);
        console.log("Converted Mobiledoc:", JSON.stringify(mobiledocObj, null, 2));
    } catch (conversionError) {
        console.error("Error converting HTML to Mobiledoc:", conversionError);
        throw conversionError;
    }

    return JSON.stringify(mobiledocObj);
};

app.post('/create-post', async (req, res) => {
    try {
        console.log("Request received:", req.body);
        const { title, content } = req.body;

        if (!title || !content) {
            return res.status(400).json({ success: false, error: "Title and content are required." });
        }

        // Process the HTML content: upload images and convert to Mobiledoc
        const mobiledocContent = await processHtmlContent(content, api);
        // Create the new post in Ghost using HTML source
        const newPost = await api.posts.add({
            title: title,
            html: content, // Use original HTML content
            source: 'html',
            status: 'draft'
        }, {
            source: 'html' // Specify source=html in query params
        });

        console.log(`Ghost post created: ${newPost.url}`);
        res.json({ success: true, post: newPost });
    } catch (error) {
        console.error("Error creating post:", error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`HTML Ghost bridge service running on port ${PORT}`);
});
