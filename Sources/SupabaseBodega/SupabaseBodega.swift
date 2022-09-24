import Bodega
import Foundation

public actor SupabaseStorageEngine: StorageEngine {
  private struct StoredValue: Codable {
    let key: String
    let data: Data
  }

  public enum Error: LocalizedError {
    case unaccetableStatusCode(_ code: Int)

    public var errorDescription: String? {
      switch self {
      case let .unaccetableStatusCode(code):
        return "Unaccetable status code: \(code)"
      }
    }
  }

  let session: URLSession
  let url: URL

  let encoder: JSONEncoder
  let decoder: JSONDecoder

  public init(url: URL, table: String, apiKey: String) {
    let configuration = URLSessionConfiguration.default
    configuration.httpAdditionalHeaders = [
      "apikey": apiKey,
      "Authorization": "Bearer \(apiKey)",
      "Content-Type": "application/json",
    ]
    session = URLSession(configuration: configuration)
    self.url = url.appendingPathComponent("rest/v1/\(table)")

    let encoder = JSONEncoder()
    encoder.keyEncodingStrategy = .convertToSnakeCase

    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase

    let dateFormatter = ISO8601DateFormatter()
    dateFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    decoder.dateDecodingStrategy = .custom { decoder in
      let string = try decoder.singleValueContainer().decode(String.self)
      return dateFormatter.date(from: string)!
    }

    self.encoder = encoder
    self.decoder = decoder
  }

  public func write(_ data: Data, key: Bodega.CacheKey) async throws {
    try await write([(key, data)])
  }

  public func write(_ dataAndKeys: [(key: Bodega.CacheKey, data: Data)]) async throws {
    let body = dataAndKeys.map { key, data in
      StoredValue(key: key.value, data: data)
    }

    let request = Request(
      method: "POST",
      body: try? encoder.encode(body),
      headers: [
        "Prefer": "resolution=merge-duplicates",
      ]
    )

    try await send(request)
  }

  public func read(key: Bodega.CacheKey) async -> Data? {
    let request = Request(
      method: "GET",
      query: [
        URLQueryItem(name: "select", value: "data"),
        URLQueryItem(name: "key", value: "eq.\(key.value)"),
      ]
    )

    do {
      let (data, _) = try await send(request)
      struct Container: Decodable {
        let data: Data
      }
      let container = try decoder.decode([Container].self, from: data)
      return container.first?.data
    } catch {
      print(error)
      return nil
    }
  }

  public func read(keys: [CacheKey]) async -> [Data] {
    let request = Request(
      method: "GET",
      query: [
        URLQueryItem(name: "select", value: "data"),
        URLQueryItem(name: "key", value: "in.(\(keys.map(\.value).joined(separator: ",")))"),
      ]
    )

    do {
      let (data, _) = try await send(request)
      struct Container: Decodable {
        let data: Data
      }
      let container = try decoder.decode([Container].self, from: data)
      return container.map(\.data)
    } catch {
      print(error)
      return []
    }
  }

  public func readDataAndKeys(keys: [Bodega.CacheKey]) async
    -> [(key: Bodega.CacheKey, data: Data)]
  {
    let request = Request(
      method: "GET",
      query: [
        URLQueryItem(name: "select", value: "key,data"),
        URLQueryItem(name: "key", value: "in.(\(keys.map(\.value).joined(separator: ",")))"),
      ]
    )

    do {
      let (data, _) = try await send(request)
      let values = try decoder.decode([StoredValue].self, from: data)

      return values.map {
        (CacheKey(verbatim: $0.key), $0.data)
      }
    } catch {
      print(error)
      return []
    }
  }

  public func readAllData() async -> [Data] {
    let request = Request(
      method: "GET",
      query: [
        URLQueryItem(name: "select", value: "data"),
      ]
    )
    do {
      let (data, _) = try await send(request)
      struct Container: Decodable {
        let data: Data
      }
      let container = try decoder.decode([Container].self, from: data)
      return container.map(\.data)
    } catch {
      print(error)
      return []
    }
  }

  public func readAllDataAndKeys() async -> [(key: CacheKey, data: Data)] {
    let request = Request(
      method: "GET",
      query: [
        URLQueryItem(name: "select", value: "key,data"),
      ]
    )

    do {
      let (data, _) = try await send(request)
      let values = try decoder.decode([StoredValue].self, from: data)

      return values.map {
        (CacheKey(verbatim: $0.key), $0.data)
      }
    } catch {
      print(error)
      return []
    }
  }

  public func remove(key: CacheKey) async throws {
    let request = Request(
      method: "DELETE",
      query: [
        URLQueryItem(name: "key", value: "eq.\(key.value)"),
      ]
    )

    try await send(request)
  }

  public func remove(keys: [CacheKey]) async throws {
    let request = Request(
      method: "DELETE",
      query: [
        URLQueryItem(name: "key", value: "in.(\(keys.map(\.value).joined(separator: ",")))"),
      ]
    )

    try await send(request)
  }

  public func removeAllData() async throws {
    let request = Request(
      method: "DELETE",
      query: [
        URLQueryItem(name: "key", value: "neq.\(UUID().uuidString)"),
      ]
    )
    try await send(request)
  }

  public func keyExists(_ key: Bodega.CacheKey) async -> Bool {
    let request = Request(
      method: "HEAD",
      query: [
        URLQueryItem(name: "key", value: "eq.\(key.value)"),
      ],
      headers: [
        "Prefer": "count=exact",
      ]
    )

    do {
      let (_, response) = try await send(request)
      guard let contentRange = response.value(forHTTPHeaderField: "Content-Range") else {
        throw URLError(.badServerResponse)
      }

      let count = contentRange.split(separator: "/").last.flatMap { Int($0) } ?? 0
      return count > 0
    } catch {
      print(error)
      return false
    }
  }

  public func keyCount() async -> Int {
    let request = Request(
      method: "HEAD",
      headers: [
        "Prefer": "count=exact",
      ]
    )

    do {
      let (_, response) = try await send(request)
      guard let contentRange = response.value(forHTTPHeaderField: "Content-Range") else {
        throw URLError(.badServerResponse)
      }

      let count = contentRange.split(separator: "/").last
      return count.flatMap { Int($0) } ?? 0
    } catch {
      print(error)
      return 0
    }
  }

  public func allKeys() async -> [Bodega.CacheKey] {
    let request = Request(
      method: "GET",
      query: [
        URLQueryItem(name: "select", value: "key"),
      ]
    )
    do {
      let (data, _) = try await send(request)

      struct KeyResponse: Decodable {
        let key: String
      }

      let keys = try decoder.decode([KeyResponse].self, from: data)

      return keys.map { CacheKey(verbatim: $0.key) }
    } catch {
      return []
    }
  }

  public func createdAt(key: Bodega.CacheKey) async -> Date? {
    let request = Request(
      method: "GET",
      query: [
        URLQueryItem(name: "select", value: "created_at"),
        URLQueryItem(name: "key", value: "eq.\(key.value)"),
      ]
    )

    do {
      let (data, _) = try await send(request)
      struct Container: Decodable {
        let createdAt: Date
      }
      let values = try decoder.decode([Container].self, from: data)
      return values.first?.createdAt
    } catch {
      print(error)
      return nil
    }
  }

  public func updatedAt(key: Bodega.CacheKey) async -> Date? {
    let request = Request(
      method: "GET",
      query: [
        URLQueryItem(name: "select", value: "updated_at"),
        URLQueryItem(name: "key", value: "eq.\(key.value)"),
      ]
    )

    do {
      let (data, _) = try await send(request)
      struct Container: Decodable {
        let updatedAt: Date
      }
      let values = try decoder.decode([Container].self, from: data)
      return values.first?.updatedAt
    } catch {
      print(error)
      return nil
    }
  }

  @discardableResult
  private func send(_ request: Request) async throws -> (Data, HTTPURLResponse) {
    let (data, response) = try await session.data(for: request, withURL: url)
    let httpResponse = try validate(response)
    return (data, httpResponse)
  }

  private func validate(_ response: URLResponse) throws -> HTTPURLResponse {
    guard let httpResponse = response as? HTTPURLResponse else {
      throw URLError(.badServerResponse)
    }

    guard 200 ..< 300 ~= httpResponse.statusCode else {
      throw Error.unaccetableStatusCode(httpResponse.statusCode)
    }

    return httpResponse
  }
}

struct Request {
  let method: String
  var query: [URLQueryItem] = []
  var body: Data?
  var headers: [String: String] = [:]
}

extension URLSession {
  func data(for request: Request, withURL url: URL) async throws -> (Data, URLResponse) {
    var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
    if !request.query.isEmpty {
      components.queryItems = request.query
    }
    let url = components.url!
    var urlRequest = URLRequest(url: url)
    urlRequest.httpBody = request.body
    urlRequest.httpMethod = request.method
    request.headers.forEach {
      urlRequest.setValue($0.value, forHTTPHeaderField: $0.key)
    }
    return try await data(for: urlRequest)
  }
}
