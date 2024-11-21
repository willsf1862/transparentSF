// dataLoader.js
import fs from 'fs';

export const loadData = (filePath) => {
  try {
    const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return data;
  } catch (err) {
    console.error('Error loading data:', err);
    return [];
  }
};