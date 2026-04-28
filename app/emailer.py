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

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as smtp:
            if settings.smtp_use_tls:
                smtp.starttls()
            if settings.smtp_username:
                smtp.login(settings.smtp_username, settings.smtp_password)
            smtp.send_message(message)
        return True
    except Exception:
        logger.exception("Could not send email to %s", to_email)
        return False


def send_verification_email(settings: Settings, to_email: str, verify_url: str, code: str) -> bool:
    return send_email(
        settings,
        to_email,
        "Verify your SwarmPanel email",
        "Welcome to SwarmPanel.\n\n"
        f"Your verification code is:\n\n{code}\n\n"
        "Enter this code in SwarmPanel to verify your email address.\n\n"
        "You can also verify by opening the link below:\n\n"
        f"{verify_url}\n\n"
        "If you did not create this account, you can ignore this message.",
    )


def send_image_gallery_verification_email(settings: Settings, to_email: str, verify_url: str, code: str) -> bool:
    return send_email(
        settings,
        to_email,
        "Verify your Image Gallery email",
        "Welcome to Image Gallery.\n\n"
        f"Your verification code is:\n\n{code}\n\n"
        "Enter this code in Image Gallery to verify your email address.\n\n"
        "You can also verify by opening the link below:\n\n"
        f"{verify_url}\n\n"
        "If you did not create this account, you can ignore this message.",
    )
