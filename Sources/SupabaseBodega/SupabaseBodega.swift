import Bodega
import Foundation
import OSLog

public actor SupabaseStorageEngine: StorageEngine {
  struct StoredValue: Codable {
    let key: String
    let data: Data
  }

  let logger = Logger(
    subsystem: "co.binarsycraping.supabase-bodega",
    category: "\(SupabaseStorageEngine.self)"
  )

  let session: URLSession
  let url: URL

  let encoder: JSONEncoder
  let decoder: JSONDecoder

  public init(url: URL, apiKey: String, table: String) {
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

      guard let date = dateFormatter.date(from: string) else {
        throw DecodingError.dataCorrupted(
          .init(
            codingPath: decoder.codingPath,
            debugDescription: "Unexpected date format found: \(string)"
          )
        )
      }

      return date
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

    do {
      try await send(request)
    } catch {
      logger
        .error(
          "Error writing data and keys for keys '\(body.map(\.key))', error: \(String(describing: error))"
        )
      throw error
    }
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
      struct Container: Decodable {
        let data: Data
      }
      let (values, _) = try await send(request, as: [Container].self)
      return values.first?.data
    } catch {
      logger.error("Error reading data for key '\(key.value)', error: \(String(describing: error))")
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
      struct Container: Decodable {
        let data: Data
      }
      let (values, _) = try await send(request, as: [Container].self)
      return values.map(\.data)
    } catch {
      logger
        .error(
          "Error reading data for keys '\(keys.map(\.value))', error: \(String(describing: error))"
        )
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
      let (values, _) = try await send(request, as: [StoredValue].self)
      return values.map {
        (CacheKey(verbatim: $0.key), $0.data)
      }
    } catch {
      logger
        .error(
          "Error reading data and keys for keys '\(keys.map(\.value))', error: \(String(describing: error))"
        )
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
      struct Container: Decodable {
        let data: Data
      }
      let (values, _) = try await send(request, as: [Container].self)
      return values.map(\.data)
    } catch {
      logger.error("Error reading all data, error: \(String(describing: error))")
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
      let (values, _) = try await send(request, as: [StoredValue].self)
      return values.map {
        (CacheKey(verbatim: $0.key), $0.data)
      }
    } catch {
      logger.error("Error reading all data and keys, error: \(String(describing: error))")
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

    do {
      try await send(request)
    } catch {
      logger.error("Error removing key '\(key.value)', error: \(String(describing: error))")
      throw error
    }
  }

  public func remove(keys: [CacheKey]) async throws {
    let request = Request(
      method: "DELETE",
      query: [
        URLQueryItem(name: "key", value: "in.(\(keys.map(\.value).joined(separator: ",")))"),
      ]
    )

    do {
      try await send(request)
    } catch {
      logger
        .error("Error removing keys '\(keys.map(\.value))', error: \(String(describing: error))")
      throw error
    }
  }

  public func removeAllData() async throws {
    let request = Request(
      method: "DELETE",
      query: [
        URLQueryItem(name: "key", value: "neq.\(UUID().uuidString)"),
      ]
    )
    do {
      try await send(request)
    } catch {
      logger.error("Error removing all data, error: \(String(describing: error))")
      throw error
    }
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
        logger.error("Missing 'Content-Range' header from response.")
        throw URLError(.badServerResponse)
      }

      let count = contentRange.split(separator: "/").last.flatMap { Int($0) }
      guard let count else {
        logger.error("Wrong format for 'Content-Range' found: \(contentRange)")
        return false
      }

      return count > 0
    } catch {
      logger
        .error("Error checking if key '\(key.value)' exists, error: \(String(describing: error))")
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
        logger.error("Missing 'Content-Range' header from response.")
        throw URLError(.badServerResponse)
      }

      let count = contentRange.split(separator: "/").last.flatMap { Int($0) }
      guard let count else {
        logger.error("Wrong format for 'Content-Range' found: \(contentRange)")
        return 0
      }

      return count
    } catch {
      logger.error("Error fetching key count, error: \(String(describing: error))")
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
      struct KeyResponse: Decodable {
        let key: String
      }
      let (keys, _) = try await send(request, as: [KeyResponse].self)
      return keys.map { CacheKey(verbatim: $0.key) }
    } catch {
      logger.error("Error fetching all keys, error: \(String(describing: error))")
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
      struct Container: Decodable {
        let createdAt: Date
      }
      let (values, _) = try await send(request, as: [Container].self)
      return values.first?.createdAt
    } catch {
      logger
        .error(
          "Error fetching 'created_at' from key '\(key.value)', error: \(String(describing: error))"
        )
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
      struct Container: Decodable {
        let updatedAt: Date
      }

      let (values, _) = try await send(request, as: [Container].self)
      return values.first?.updatedAt
    } catch {
      logger
        .error(
          "Error fetching 'updated_at' from key '\(key.value)', error: \(String(describing: error))"
        )
      return nil
    }
  }

  @discardableResult
  private func send(_ request: Request) async throws -> (Data, HTTPURLResponse) {
    let (data, response) = try await session.data(for: request, withURL: url)
    let httpResponse = try validate(response)
    return (data, httpResponse)
  }

  private func send<T: Decodable>(
    _ request: Request,
    as _: T.Type
  ) async throws -> (T, HTTPURLResponse) {
    let (data, response) = try await send(request)
    let decodedValue = try decoder.decode(T.self, from: data)
    return (decodedValue, response)
  }

  private func validate(_ response: URLResponse) throws -> HTTPURLResponse {
    guard
      let httpResponse = response as? HTTPURLResponse,
      200 ..< 300 ~= httpResponse.statusCode
    else {
      throw URLError(.badServerResponse)
    }

    return httpResponse
  }
}
