using System;

namespace codynolf.fasterkv.abstractions;

public interface IFasterKvService<K, V>
{
  void Dispose();

  V Read(K key);
  Task<V> ReadAsync(K key);

  void Upsert(K key, V value);
  Task UpsertAsync(K key, V value);

  void RMW(K key, V value);
  Task RMWAsync(K key, V value);
}
