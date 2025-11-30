# Security Policy

## Supported Versions

Ceres is currently in early development (pre-v1.0). Security updates will be provided for the latest development version.

| Version | Supported          |
| ------- | ------------------ |
| main    | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in Ceres, please report it privately:

1. **Do not** open a public issue
2. Email the maintainers or use GitHub's private vulnerability reporting
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

We will respond within 48 hours and work with you to address the issue.

## Security Considerations

When using Ceres:

- **API Keys**: Never commit OpenAI API keys or database credentials to version control
- **Database**: Use strong passwords for PostgreSQL and restrict network access
- **Input Validation**: Be cautious when harvesting from untrusted data portals
- **Dependencies**: Keep Rust dependencies updated with `cargo update`

## Disclosure Policy

Once a security issue is fixed, we will:
1. Release a patch
2. Publish a security advisory
3. Credit the reporter (unless they prefer anonymity)
