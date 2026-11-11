import Foundation
import Security

struct Connection: Equatable {
    let url: URL
    let token: String

    init(server: String, token: String) throws {
        guard let parts = URLComponents(string: server.trimmingCharacters(in: .whitespacesAndNewlines)),
              parts.scheme?.lowercased() == "https", parts.host != nil,
              parts.user == nil, parts.password == nil, parts.query == nil, parts.fragment == nil,
              parts.path.isEmpty || parts.path == "/", let url = parts.url
        else { throw ConnectionError.invalidServer }
        let token = token.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !token.isEmpty, !token.unicodeScalars.contains(where: CharacterSet.whitespacesAndNewlines.contains)
        else { throw ConnectionError.invalidToken }
        self.url = url
        self.token = token
    }

    private static let key: [String: String] = [
        kSecClass as String: kSecClassGenericPassword as String,
        kSecAttrService as String: "dev.podible.tv.connection",
        kSecAttrAccount as String: "current",
    ]

    static func load() -> Self? {
        var query: [String: Any] = key
        query[kSecReturnData as String] = true
        query[kSecMatchLimit as String] = kSecMatchLimitOne
        var result: CFTypeRef?
        guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
              let data = result as? Data, let values = try? JSONDecoder().decode([String].self, from: data),
              values.count == 2 else { return nil }
        return try? Self(server: values[0], token: values[1])
    }

    func save() throws {
        let data = try JSONEncoder().encode([url.absoluteString, token])
        let values = [kSecValueData as String: data]
        var status = SecItemUpdate(Self.key as CFDictionary, values as CFDictionary)
        if status == errSecItemNotFound {
            var newItem: [String: Any] = Self.key
            newItem[kSecValueData as String] = data
            newItem[kSecAttrAccessible as String] = kSecAttrAccessibleWhenUnlockedThisDeviceOnly
            status = SecItemAdd(newItem as CFDictionary, nil)
        }
        guard status == errSecSuccess else { throw ConnectionError.keychain }
    }

    static func remove() throws {
        let status = SecItemDelete(key as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else { throw ConnectionError.keychain }
    }
}

private enum ConnectionError: LocalizedError {
    case invalidServer, invalidToken, keychain
    var errorDescription: String? {
        switch self {
        case .invalidServer: "Enter an HTTPS server address, without a path or query."
        case .invalidToken: "Enter a valid Podible session token."
        case .keychain: "Apple TV could not update the saved connection."
        }
    }
}
