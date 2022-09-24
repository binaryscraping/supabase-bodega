import Foundation

struct Request {
  let method: String
  var query: [URLQueryItem] = []
  var body: Data?
  var headers: [String: String] = [:]
}

extension URLSession {
  func data(for request: Request, withURL url: URL) async throws -> (Data, URLResponse) {
    guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
      throw URLError(.badURL)
    }

    if !request.query.isEmpty {
      components.queryItems = request.query
    }

    guard let url = components.url else {
      throw URLError(.badURL)
    }

    var urlRequest = URLRequest(url: url)
    urlRequest.httpBody = request.body
    urlRequest.httpMethod = request.method
    request.headers.forEach {
      urlRequest.setValue($0.value, forHTTPHeaderField: $0.key)
    }
    return try await data(for: urlRequest)
  }
}
