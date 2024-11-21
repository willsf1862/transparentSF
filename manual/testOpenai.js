import OpenAI from "openai";
import dotenv from 'dotenv';

dotenv.config();
 
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
  dangerouslyAllowBrowser: true,
});
console.log("running")
testOpenAI();

async function testOpenAI() {
    try {
        const response =await openai.chat.completions.create({
            model: "gpt-4o",
            messages: [
                { role: "system", content: "You are a helpful assistant." },
                {
                    role: "user",
                    content: "Write a haiku about recursion in programming.",
                },
            ],
        });
        console.log(response.choices[0].message);
    } catch (error) {
        console.error("Error:", error);
    }
}