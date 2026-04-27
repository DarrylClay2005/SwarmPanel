import logging
import smtplib
from email.message import EmailMessage

from .config import Settings


logger = logging.getLogger("swarm_panel.email")


def smtp_configured(settings: Settings) -> bool:
    return bool(settings.smtp_host and settings.smtp_from_email)


def send_email(settings: Settings, to_email: str, subject: str, body: str) -> bool:
    if not smtp_configured(settings):
        logger.warning("SMTP is not configured; could not send email to %s", to_email)
        return False

    message = EmailMessage()
    message["From"] = settings.smtp_from_email
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body)

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as smtp:
        if settings.smtp_use_tls:
            smtp.starttls()
        if settings.smtp_username:
            smtp.login(settings.smtp_username, settings.smtp_password)
        smtp.send_message(message)
    return True


def send_verification_email(settings: Settings, to_email: str, verify_url: str) -> bool:
    return send_email(
        settings,
        to_email,
        "Verify your SwarmPanel email",
        "Welcome to SwarmPanel.\n\n"
        "Verify this email address by opening the link below:\n\n"
        f"{verify_url}\n\n"
        "If you did not create this account, you can ignore this message.",
    )
