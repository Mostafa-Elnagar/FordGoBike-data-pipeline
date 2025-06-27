const express = require("express");
const nodemailer = require("nodemailer");
const cors = require("cors");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3000;

// Improved CORS configuration - more secure
app.use(
  cors({
    origin: process.env.ALLOWED_ORIGINS ? process.env.ALLOWED_ORIGINS.split(',') : ["http://localhost:3000", "http://localhost:5173"],
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type"],
  })
);
app.use(express.json());

// Input validation middleware
const validateEmailInput = (req, res, next) => {
  const { name, email, subject, message, receiver_email } = req.body;
  
  if (!name || !email || !subject || !message || !receiver_email) {
    return res.status(400).json({ 
      message: "All fields are required: name, email, subject, message, receiver_email" 
    });
  }
  
  // Basic email validation
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email) || !emailRegex.test(receiver_email)) {
    return res.status(400).json({ 
      message: "Invalid email format" 
    });
  }
  
  // Check for reasonable length limits
  if (name.length > 100 || subject.length > 200 || message.length > 2000) {
    return res.status(400).json({ 
      message: "Input too long. Name: max 100 chars, Subject: max 200 chars, Message: max 2000 chars" 
    });
  }
  
  next();
};

app.post("/send", validateEmailInput, async (req, res) => {
  const { name, email, subject, message, receiver_email } = req.body;

  try {
    // Check if environment variables are set
    if (!process.env.EMAIL_USER || !process.env.EMAIL_PASS) {
      console.error("Missing email configuration");
      return res.status(500).json({ 
        message: "Email service not configured. Please check environment variables." 
      });
    }

    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS,
      },
    });

    // Verify transporter configuration
    await transporter.verify();

    await transporter.sendMail({
      from: `"${name}" <${process.env.EMAIL_USER}>`, // Use configured email as sender
      replyTo: email, // Set reply-to as the contact form email
      to: receiver_email,
      subject: `Airflow Contact: ${subject}`,
      text: message, // fallback for plain-text clients
      html: `
    <div style="font-family: Arial, sans-serif; padding: 20px; color: #333;">
      <h2 style="color: #0BCEAF;">reciver from airflow</h2>
      <p><strong>Name:</strong> ${name}</p>
      <p><strong>Email:</strong> ${email}</p>
      <p><strong>Subject:</strong> ${subject}</p>
      <p><strong>Message:</strong></p>
      <div style="background: #f9f9f9; padding: 10px; border-left: 4px solid #0BCEAF;">
        <pre style="white-space: pre-wrap; font-family: monospace;">${message}</pre>
      </div>
      <hr style="margin-top: 20px;" />
      <small style="color: #888;">This message was sent via your Airflow site contact form.</small>
    </div>
  `,
    });

    res.status(200).json({ message: "Email sent successfully!" });
  } catch (error) {
    console.error("Email sending error:", error);
    
    // Provide more specific error messages
    let errorMessage = "Failed to send email.";
    if (error.code === 'EAUTH') {
      errorMessage = "Authentication failed. Please check your email credentials.";
    } else if (error.code === 'ECONNECTION') {
      errorMessage = "Connection failed. Please check your internet connection.";
    } else if (error.code === 'ETIMEDOUT') {
      errorMessage = "Request timed out. Please try again.";
    }
    
    res.status(500).json({ message: errorMessage });
  }
});

app.get("/", (_, res) => {
  res.send("Email API is running.");
});

// Health check endpoint
app.get("/health", (_, res) => {
  res.json({ 
    status: "healthy", 
    timestamp: new Date().toISOString(),
    emailConfigured: !!(process.env.EMAIL_USER && process.env.EMAIL_PASS)
  });
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
  console.log(`Health check available at: http://localhost:${port}/health`);
});
