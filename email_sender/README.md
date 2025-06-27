# Email API Backend with Node.js and Nodemailer

## Table of Contents

- [Overview](#overview)
- [My Process](#my-process)
  - [Built With](#built-with)
- [How to Use](#how-to-use)
- [Recent Fixes & Improvements](#recent-fixes--improvements)
- [Deploy on Vercel](#deploy-on-vercel)
- [Configure Gmail for Nodemailer](#configure-gmail-for-nodemailer)
- [Author](#author)

---

## Overview

This is a simple email-sending API built with Node.js and Express using **Nodemailer**. It can be deployed to **Vercel** and used with any frontend (like your portfolio) to send emails securely via Gmail using environment variables and a serverless backend.

---

## My Process

### Built With

- [Node.js](https://nodejs.org/)
- [Express](https://expressjs.com/)
- [Nodemailer](https://nodemailer.com/about/)
- [dotenv](https://www.npmjs.com/package/dotenv)
- [Vercel](https://vercel.com/) – for deployment

---

## How to Use

1. **Fork** this repository.
2. **Clone** it to your machine:
   ```bash
   git clone https://github.com/your-username/email-sender.git
   ```
3. Navigate to the project folder:
   ```bash
   cd email-sender
   ```
4. Install dependencies:
   ```bash
   npm install
   ```
5. Create a `.env` file and add your environment variables:
   ```env
   PORT=5000
   EMAIL_USER=your_email@gmail.com
   EMAIL_PASS=your_app_password
   ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173,https://yourdomain.com
   ```
6. Start the server locally:
   ```bash
   npm start
   ```
7. Make a POST request to:
   ```
   http://localhost:5000/send
   ```
   With a body like:
   ```json
   {
     "name": "John Doe",
     "email": "johndoe@example.com",
     "subject": "Hello!",
     "message": "Testing the email service.",
     "receiver_email": "your_email@gmail.com"
   }
   ```

### API Endpoints

- `GET /` - Check if API is running
- `GET /health` - Health check with configuration status
- `POST /send` - Send email (requires validation)

---

## Recent Fixes & Improvements

### ✅ Fixed Issues:
1. **Gmail Authentication**: Added proper error handling for authentication failures
2. **Input Validation**: Added comprehensive validation for all required fields
3. **Security**: Improved CORS configuration to restrict origins
4. **Error Handling**: Better error messages with specific failure reasons
5. **Email Format**: Improved email formatting with proper sender/reply-to headers

### 🔧 New Features:
- Input validation middleware
- Health check endpoint
- Better error messages
- Email length limits
- CORS origin restrictions
- Transporter verification

### 📝 Important Notes:
- **You must use a Gmail App Password** (not your regular password)
- See `GMAIL_SETUP.md` for detailed setup instructions
- The API now validates all inputs before processing
- CORS is now restricted to specified origins for security

---

## Deploy on Vercel

You can deploy this API to **Vercel** in a few steps:

1. Push the project to GitHub.
2. Go to [https://vercel.com/new](https://vercel.com/new).
3. Import your GitHub repository.
4. When prompted, add your environment variables:
   - `EMAIL_USER` = your Gmail address
   - `EMAIL_PASS` = your Gmail app password
   - `ALLOWED_ORIGINS` = your frontend domains (comma-separated)
   - `PORT` is not needed (Vercel handles it)
5. Click **Deploy**.

Vercel will build and host the backend. You'll be able to send requests to:

```bash
https://your-vercel-project-name.vercel.app/send
```

---

## Configure Gmail for Nodemailer

By default, Gmail will block less secure apps. To fix that:

1. Go to your [Google Account Security Settings](https://myaccount.google.com/security).
2. Enable **2-Step Verification**.
3. Go to the [App Passwords page](https://myaccount.google.com/apppasswords).
4. Generate a new App Password:
   - Select **Mail**
   - Select **Other** and type "Nodemailer"
5. Google will generate a 16-character password like:
   ```
   qwph ztsa cgws amgd
   ```
6. Remove spaces before putting it in your `.env`:
   ```env
   EMAIL_PASS=qwphztsacgwsamgd
   ```

✅ Now you're all set!

**For detailed step-by-step instructions, see `GMAIL_SETUP.md`**


---

## Credits & Acknowledgements

This module is originally based on [KareemEhab/email-sender](https://github.com/KareemEhab/email-sender).  
I have made some refactoring and adjustments to better suit this project.

