import CoreImage.CIFilterBuiltins
import SwiftUI

struct PairingQRCode: View {
    let url: URL

    private var code: CGImage? {
        let filter = CIFilter.qrCodeGenerator()
        filter.message = Data(url.absoluteString.utf8)
        filter.correctionLevel = "M"
        guard let image = filter.outputImage else { return nil }
        return CIContext().createCGImage(image, from: image.extent)
    }

    var body: some View {
        if let code {
            Image(decorative: code, scale: 1)
                .interpolation(.none)
                .resizable()
                .scaledToFit()
                .frame(width: 280, height: 280)
                .padding(22)
                .background(.white, in: RoundedRectangle(cornerRadius: 12))
                .accessibilityLabel("Scan this QR code to sign in")
        }
    }
}
