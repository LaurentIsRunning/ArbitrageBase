import smtplib

class SmtpClient(object):
    """Smtp client to send report emails"""

    def __init__(self, **kwargs):
      self.fromaddr = 'your reporting email address'
      self.toaddrs  = ['destination profit addresses']
      self.toErrorAddrs  = ['destination error addresses']
      self.username = 'your reporting email address'
      self.password = 'your reporting emai password'

    def sendException(self, exception):
      self.sendMessage("Exception " + type(exception).__name__, str(exception), self.toErrorAddrs)

    def sendProfit(self, operationProfitData, totalProfit):
      msg = "\r\n".join([
        'Total profit : ' + str(totalProfit),
        'Operation profits :',
        *[" - " + str(p) for p in operationProfitData]
        ])

      self.sendMessage('Profit Occured!', msg, self.toaddrs)

    def sendMessage(self, subject, message, adresses):
      self.server = smtplib.SMTP('smtp.gmail.com:587')
      self.server.ehlo()
      self.server.starttls()
      self.server.login(self.username,self.password)

      for toaddr in adresses : 
        msg = "\r\n".join([
        "From: " + self.fromaddr,
        "To: " + toaddr,
        "Subject: " + subject,
        "",
        message])
        self.server.sendmail(self.fromaddr, toaddr, msg)

      self.server.quit()
