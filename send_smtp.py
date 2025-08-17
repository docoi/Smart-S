import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import ssl
from datetime import datetime

def send_email(to_email, subject, body, from_name="PFP Fire Protection"):
    """Send email via SMTP with proper formatting"""
    
    smtp_email = os.getenv('SMTP_EMAIL')
    smtp_password = os.getenv('SMTP_PASSWORD')
    
    if not smtp_email or not smtp_password:
        print("SMTP credentials not found in environment variables")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = f"{from_name} <{smtp_email}>"
        msg['To'] = to_email
        msg['Subject'] = subject
        
        # Format the email body properly
        formatted_body = format_email_body(body)
        
        # Convert body to HTML
        html_body = convert_text_to_html(formatted_body)
        
        # Attach HTML body
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        # Gmail SMTP configuration
        smtp_server = "smtp.gmail.com"
        port = 587
        
        # Create secure connection and send
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()
        server.login(smtp_email, smtp_password)
        
        text = msg.as_string()
        server.sendmail(smtp_email, to_email, text)
        server.quit()
        
        print(f"✅ Email sent successfully to {to_email}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to send email to {to_email}: {e}")
        return False

def format_email_body(body):
    """
    Format email body with proper line breaks and spacing
    """
    import re
    
    # Remove any existing excessive line breaks
    body = re.sub(r'\n{3,}', '\n\n', body)
    
    # Ensure proper spacing after periods that end sentences
    body = re.sub(r'\.([A-Z])', r'.\n\n\1', body)
    
    # Add line breaks after greeting
    body = re.sub(r'(Hi [^,]+,)', r'\1\n\n', body)
    
    # Add line breaks before "Best," or "Regards," 
    body = re.sub(r'([.!?])(\s*)(Best,|Regards,|Kind regards,)', r'\1\n\n\2\3', body)
    
    # Add line breaks before signature section
    body = re.sub(r'([.!?])(\s*)(PFP|Fire Protection)', r'\1\n\n\2\3', body)
    
    # Ensure proper spacing around phone numbers and websites
    body = re.sub(r'(\S)(📞|\🌐)', r'\1\n\n\2', body)
    
    # Clean up any triple line breaks that might have been created
    body = re.sub(r'\n{3,}', '\n\n', body)
    
    return body.strip()

def convert_text_to_html(text_body):
    """Convert plain text email to HTML format with proper line breaks"""
    
    # Replace line breaks with HTML breaks
    html_body = text_body.replace('\n', '<br>\n')
    
    # Convert double line breaks to paragraphs for better spacing
    html_body = html_body.replace('<br>\n<br>\n', '</p>\n<p>')
    
    # Wrap in basic HTML structure with better styling
    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>PFP Fire Protection</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                margin: 0;
                padding: 0;
            }}
            .email-container {{
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
            }}
            .email-content {{
                background: #ffffff;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            p {{
                margin: 0 0 15px 0;
                line-height: 1.6;
            }}
            .signature {{
                margin-top: 25px;
                padding-top: 20px;
                border-top: 1px solid #eee;
                font-size: 14px;
                line-height: 1.8;
            }}
            .footer {{
                font-size: 12px;
                color: #666;
                text-align: center;
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #eee;
            }}
            a {{
                color: #2563eb;
                text-decoration: none;
            }}
            a:hover {{
                text-decoration: underline;
            }}
        </style>
    </head>
    <body>
        <div class="email-container">
            <div class="email-content">
                <p>{html_body}</p>
            </div>
            
            <div class="footer">
                <p>PFP Fire Protection | Professional Fire Safety Services</p>
                <p>📧 This email was sent to you because we believe our fire protection services could benefit your business.</p>
                <p>🔗 Visit us: <a href="https://p-f-p.co.uk/">https://p-f-p.co.uk/</a></p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html_template

def send_bulk_emails(contacts_df, delay_between_emails=30):
    """Send emails to multiple contacts with delays"""
    import time
    import pandas as pd
    
    sent_count = 0
    failed_count = 0
    
    for index, row in contacts_df.iterrows():
        email = row.get('contact_email')
        subject = row.get('email_subject')
        body = row.get('email_body')
        contact_name = row.get('contact_name')
        
        if email and subject and body:
            print(f"Sending email {index + 1} to {contact_name} ({email})...")
            
            success = send_email(email, subject, body)
            
            if success:
                sent_count += 1
                # Log successful send
                log_email_send(row, success=True)
            else:
                failed_count += 1
                # Log failed send
                log_email_send(row, success=False)
            
            # Delay between emails to avoid spam detection
            if index < len(contacts_df) - 1:  # Don't delay after last email
                print(f"Waiting {delay_between_emails} seconds before next email...")
                time.sleep(delay_between_emails)
        else:
            print(f"Skipping {contact_name} - missing email data")
            failed_count += 1
    
    print(f"\n📊 Bulk email summary:")
    print(f"✅ Sent: {sent_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📧 Total: {len(contacts_df)}")
    
    return sent_count, failed_count

def log_email_send(contact_row, success=True):
    """Log email sending results"""
    import csv
    from datetime import datetime
    
    log_file = "output/email_log.csv"
    
    # Create log entry
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'company': contact_row.get('company', ''),
        'contact_name': contact_row.get('contact_name', ''),
        'contact_email': contact_row.get('contact_email', ''),
        'subject': contact_row.get('email_subject', ''),
        'success': success,
        'status': 'sent' if success else 'failed'
    }
    
    # Check if file exists
    file_exists = os.path.exists(log_file)
    
    # Write to log file
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=log_entry.keys())
        
        # Write header if file is new
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(log_entry)

if __name__ == "__main__":
    # Test email sending
    test_email = "test@example.com"
    test_subject = "Test Email from PFP Fire Protection"
    test_body = """Hi there,

This is a test email from the PFP lead generation system.

We're testing the new formatting system to ensure emails look professional and are easy to read.

Best regards,

PFP Team

PFP Fire Protection
Professional Fire Safety Services
📞 [01908 103 303]
🌐 https://p-f-p.co.uk/"""
    
    print("Testing email send...")
    result = send_email(test_email, test_subject, test_body)
    
    if result:
        print("✅ Email test successful!")
    else:
        print("❌ Email test failed - check your SMTP settings")