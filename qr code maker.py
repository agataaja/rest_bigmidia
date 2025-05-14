import qrcode



def qr_maker():



    url = "https://www.cbw.org.br/desenvolvimento/120/relatorio-tecnico-escolar"
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(url)
    qr.make(fit=True)

    # Create an image from the QR code
    img = qr.make_image(fill_color="black", back_color="white")

    # Save the image
    img.save(f"site_desenvolvimento_escolar.png")


qr_maker()