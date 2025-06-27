const nodemailer = require("nodemailer");

module.exports = async (req, res) => {
  if (req.method !== "POST") {
    res.status(405).json({ message: "Method Not Allowed" });
    return;
  }

  const { name, email, subject, message, receiver_email } = req.body;

  if (!name || !email || !subject || !message || !receiver_email) {
    res.status(400).json({ message: "All fields are required: name, email, subject, message, receiver_email" });
    return;
  }

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email) || !emailRegex.test(receiver_email)) {
    res.status(400).json({ message: "Invalid email format" });
    return;
  }

  try {
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS,
      },
    });

    await transporter.verify();

    await transporter.sendMail({
      from: `"${name}" <${process.env.EMAIL_USER}>`,
      replyTo: email,
      to: receiver_email,
      subject: `Airflow Contact: ${subject}`,
      text: message,
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
};