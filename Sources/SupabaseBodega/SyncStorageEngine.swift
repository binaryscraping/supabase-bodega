import Bodega
import Collections
import Foundation

public actor SyncStorageEngine: StorageEngine {
  enum Operation {
    case write(Data, CacheKey)
    case writeDataAndKeys([(key: Bodega.CacheKey, data: Data)])
    case remove(CacheKey)
    case removeKeys([CacheKey])
    case removeAll
  }

  let local: StorageEngine
  let remote: StorageEngine

  public init(local: StorageEngine, remote: StorageEngine) {
    self.local = local
    self.remote = remote
    Task {
      while true {
        await sync()
        try await Task.sleep(nanoseconds: NSEC_PER_SEC * 10)
      }
    }
  }

  private var operations: Deque<Operation> = []

  public func write(_ data: Data, key: Bodega.CacheKey) async throws {
    try await local.write(data, key: key)
    operations.append(.write(data, key))
  }

  public func write(_ dataAndKeys: [(key: Bodega.CacheKey, data: Data)]) async throws {
    try await local.write(dataAndKeys)
    operations.append(.writeDataAndKeys(dataAndKeys))
  }

  public func read(key: Bodega.CacheKey) async -> Data? {
    await local.read(key: key)
  }

  public func read(keys: [CacheKey]) async -> [Data] {
    await local.read(keys: keys)
  }

  public func readDataAndKeys(
    keys: [Bodega.CacheKey]
  ) async -> [(key: Bodega.CacheKey, data: Data)] {
    await local.readDataAndKeys(keys: keys)
  }

  public func readAllData() async -> [Data] {
    await local.readAllData()
  }

  public func readAllDataAndKeys() async -> [(key: CacheKey, data: Data)] {
    await local.readAllDataAndKeys()
  }

  public func remove(key: CacheKey) async throws {
    try await local.remove(key: key)
    operations.append(.remove(key))
  }

  public func remove(keys: [CacheKey]) async throws {
    try await local.remove(keys: keys)
    operations.append(.removeKeys(keys))
  }

  public func removeAllData() async throws {
    try await local.removeAllData()
    operations.append(.removeAll)
  }

  public func keyExists(_ key: Bodega.CacheKey) async -> Bool {
    await local.keyExists(key)
  }

  public func keyCount() async -> Int {
    await local.keyCount()
  }

  public func allKeys() async -> [Bodega.CacheKey] {
    await local.allKeys()
  }

  public func createdAt(key: Bodega.CacheKey) async -> Date? {
    await local.createdAt(key: key)
  }

  public func updatedAt(key: Bodega.CacheKey) async -> Date? {
    await local.updatedAt(key: key)
  }

  private func sync() async {
    while let operation = operations.popFirst() {
      do {
        switch operation {
        case let .write(data, key):
          try await remote.write(data, key: key)
        case let .writeDataAndKeys(dataAndKeys):
          try await remote.write(dataAndKeys)
        case let .remove(key):
          try await remote.remove(key: key)
        case let .removeKeys(keys):
          try await remote.remove(keys: keys)
        case .removeAll:
          try await remote.removeAllData()
        }
      } catch {
        dump(error)
        // TODO: check if error is retryable and retry operation

        // Put operation back in queue.
        operations.prepend(operation)

        break
      }
    }
  }
}
