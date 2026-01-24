UPLOAD_MESSAGE_TEMPLATE = """
<html>
<body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6; background-color: #f4f4f4; margin: 0; padding: 0;">
  <table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center" width="100%" style="max-width: 600px; margin: auto; background-color: #ffffff; border: 1px solid #e0e0e0; border-radius: 8px;">
    <tr>
      <td style="background-color: #2c3e50; padding: 20px; text-align: center;">
        <img src="https://www.explorer.core-stack.org/static/media/newlogoWhite.49a9a6f4f7debe5a6ad8.png" alt="CoRE Stack" style="max-height: 50px;">
      </td>
    </tr>

    <tr>
      <td style="padding: 30px;">
        <h2 style="color: #2c3e50; text-align: center;">
          New File Uploaded
        </h2>

        <p style="font-size: 16px;">
          New file is uploaded by <strong>{username}</strong> from the dashboard
          for project <strong>{proj_name}</strong> (ID: {proj_id}).
          Please process compute. Attaching file.
        </p>

        <p>Thanks,</p>
        <p style="font-weight: bold;">CoRE Stack Team</p>
      </td>
    </tr>

    <tr>
      <td style="background-color: #f9f9f9; padding: 15px; text-align: center; font-size: 12px;">
        © 2025 CoRE Stack. All rights reserved.
      </td>
    </tr>
  </table>
</body>
</html>
"""
