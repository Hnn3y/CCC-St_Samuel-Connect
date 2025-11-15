# CCC Sunday School

This system provides the official, weekly Order of Service documents for all Sunday School classes. Its purpose is to ensure all staff and volunteers have clear, standardized instructions for a smooth and consistent teaching schedule every Sunday.

## Key Features

-- Automated data fetching from a Google Sheet or Excel document.

-- Email dispatch to one or multiple recipients.

Scheduled or on-demand triggers for sending messages.

Error handling and logging for tracking failed operations.

Scalable architecture suitable for future expansion (SMS, WhatsApp, API events).

## How It Works

The script connects to the spreadsheet using API credentials or local file access.

It reads and processes the required rows/columns.

It formats the extracted values into a clean email template.

The system sends the email through an SMTP server or a cloud email service.

Logs are generated for each run to track activity.

## 🛠️ Built With
- Node.js  
- Express  
- Google Sheets API  
- Nodemailer  


## 📬 Contact
If you have questions, reach out at: your.email@example.com

