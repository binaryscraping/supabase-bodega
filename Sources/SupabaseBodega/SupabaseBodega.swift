import Bodega
import Foundation

public actor SupabaseStorageEngine: StorageEngine {

  private struct StoredValue: Encodable {
    let key: String
    let data: Data
  }

  public enum Error: Swift.Error {
    case unaccetableStatusCode(_ code: Int)
  }

  let session: URLSession = .shared
  let url: URL

  public init(table: String) {
    url = URL(string: "http://localhost:5435/v1/rest/\(table)")!
  }

  public func write(_ data: Data, key: Bodega.CacheKey) async throws {
    try await write([(key, data)])
  }

  public func write(_ dataAndKeys: [(key: Bodega.CacheKey, data: Data)]) async throws {
    let body = dataAndKeys.map { key, data in
      StoredValue(key: key.value, data: data)
    }

    var request = URLRequest(url: url)
    request.setValue("resolution=merge-duplicates", forHTTPHeaderField: "Prefer")
    request.httpMethod = "POST"
    request.httpBody = try? JSONEncoder().encode(body)

    let (_, response) = try await session.data(for: request)
    try validate(response)
  }

  public func read(key: Bodega.CacheKey) async -> Data? {
    var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
    components.queryItems = [
      URLQueryItem(name: "select", value: "data"),
      URLQueryItem(name: "key", value: "eq.\(key.value)")
    ]
    let url = components.url!
    var request = URLRequest(url: url)
    request.httpMethod = "GET"

    do {
      let (data, response) = try await session.data(for: request)
      try validate(response)
      return data
    } catch {
      return nil
    }
  }

  public func read(keys: [CacheKey]) async -> [Data] {
    []
  }

  public func readDataAndKeys(keys: [Bodega.CacheKey]) async -> [(key: Bodega.CacheKey, data: Data)] {
    []
  }

  public func readAllData() async -> [Data] {
    []
  }

  public func readAllDataAndKeys() async -> [(key: CacheKey, data: Data)] {
    []
  }

  public func remove(key: CacheKey) async throws {

  }

  public func remove(keys: [CacheKey]) async throws {

  }

  public func removeAllData() async throws {
  }

  public func keyExists(_ key: Bodega.CacheKey) async -> Bool {
    true
  }

  public func keyCount() async -> Int {
    1
  }

  public func allKeys() async -> [Bodega.CacheKey] {
    []
  }

  public func createdAt(key: Bodega.CacheKey) async -> Date? {
    nil
  }

  public func updatedAt(key: Bodega.CacheKey) async -> Date? {
    nil
  }

  private func validate(_ response: URLResponse) throws {
    guard let httpResponse = response as? HTTPURLResponse else {
      throw URLError(.badServerResponse)
    }

    guard 200..<300 ~= httpResponse.statusCode else {
      throw Error.unaccetableStatusCode(httpResponse.statusCode)
    }
  }
}
