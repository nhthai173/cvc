"""
State Management Abstraction Layer
===================================

Base classes để quản lý state trong CIP processing.
Phase 1: Implementation với in-memory (dict + threading.Lock)
Phase 2: Thay thế implementation bằng Redis, không cần thay đổi code sử dụng.

Design Pattern: Strategy Pattern + Dependency Injection
"""

from abc import ABC, abstractmethod
from typing import Optional, Any, Dict
from datetime import datetime
import threading
import redis
import json
import re


class StateManager(ABC):
    """
    Abstract base class cho state management.
    """

    def _is_same(self, v1, v2) -> bool:
        """So sánh hai giá trị có cùng ý nghĩa hay không"""
        if v1 is None and v2 is None:
            return True
        if isinstance(v1, str) and v1.strip() == "" and (v2 is None or (isinstance(v2, str) and v2.strip() == "")):
            return True
        if isinstance(v2, str) and v2.strip() == "" and (v1 is None or (isinstance(v1, str) and v1.strip() == "")):
            return True
        return v1 == v2
    
    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """
        Lấy giá trị từ state store.
        
        Args:
            key: State key (e.g., "current_run", "last_data_row")
            default: Giá trị mặc định nếu key không tồn tại
            
        Returns:
            State value hoặc default
            
        Example:
            >>> current_run = state_mgr.get("current_run")
            >>> last_step = state_mgr.get("last_step", default=0)
        """
        pass
    
    @abstractmethod
    def get_json(self, state_key: str, json_key: str, default: Any = None) -> Any:
        """
        Lấy một key bên trong object JSON lưu trong state store.
        
        Args:
            state_key: State key chứa JSON object
            json_key: Key bên trong JSON object
            default: Giá trị mặc định nếu không tồn tại
            
        Returns:
            Giá trị hoặc default
            
        Example:
            >>> run_status = state_mgr.get("current_run", "status")
            >>> temp_value = state_mgr.get("sensor_data", "temperature", default=25.0)
        """
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Lưu giá trị vào state store.
        
        Args:
            key: State key
            value: Giá trị cần lưu (dict, string, int, etc.)
            ttl: Time-to-live in seconds (optional, cho Redis)
            
        Returns:
            True nếu thành công
            
        Example:
            >>> state_mgr.set("current_run", run_record)
            >>> state_mgr.set("temp_data", data, ttl=3600)
        """
        pass
    
    @abstractmethod
    def set_json(self, state_key: str, json_key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a key inside a JSON object stored at state_key.
        
        Args:
            state_key: State key containing JSON object
            json_key: Key inside the JSON object
            value: Value to set
            ttl: Time-to-live in seconds (optional, cho Redis)
            
        Returns:
            True if successful
            
        Example:
            >>> state_mgr.set("current_run", "status", "running")
            >>> state_mgr.set("sensor_data", "temperature", 26.5, ttl=3600)
        """
        pass

    def update_changes(self, key: str, data: dict) -> Optional[dict]:
        """
        Cập nhật dữ liệu và trả về các fields đã thay đổi.
        
        Args:
            key: State key
            data: Dict dữ liệu mới
            
        Returns:
            Dict các fields đã thay đổi hoặc None nếu không có thay đổi
            
        Example:
            >>> changes = state_mgr.update_changes("current_run", new_run_data)
            >>> if changes:
            >>>     print("Updated fields:", changes)
        """
        if key is None or data is None or not isinstance(data, dict):
            return None
        cache = self.get(key)
        if cache is None:
            cache = {}
        changes = {}
        
        for k, v in data.items():
            if not self._is_same(data.get(k), cache.get(k)):
                changes[k] = v
        if len(changes) > 0:
            self.set(key, {**cache, **changes})
            return changes
        return None

    @abstractmethod
    def delete(self, key: str) -> bool:
        """
        Xóa key khỏi state store.
        
        Args:
            key: State key cần xóa
            
        Returns:
            True nếu thành công
        """
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """
        Kiểm tra key có tồn tại không.
        
        Args:
            key: State key
            
        Returns:
            True nếu key tồn tại
        """
        pass
    
    @abstractmethod
    def clear(self) -> bool:
        """
        Xóa toàn bộ state (dùng khi reset).
        
        Returns:
            True nếu thành công
        """
        pass
    
    @abstractmethod
    def get_all(self, pattern: str = "*") -> Dict[str, Any]:
        """
        Lấy tất cả keys matching pattern.
        
        Args:
            pattern: Key pattern (e.g., "run:*", "step:*")
            
        Returns:
            Dict của tất cả matching keys
            
        Example:
            >>> all_runs = state_mgr.get_all("run:*")
        """
        pass
    
    @abstractmethod
    def increment(self, key: str, amount: int = 1) -> int:
        """
        Tăng giá trị counter (atomic operation).
        
        Args:
            key: Counter key
            amount: Số lượng tăng
            
        Returns:
            Giá trị mới sau khi tăng
            
        Example:
            >>> count = state_mgr.increment("processed_count")
        """
        pass
    
    @abstractmethod
    def decrement(self, key: str, amount: int = 1) -> int:
        """
        Giảm giá trị counter (atomic operation).
        
        Args:
            key: Counter key
            amount: Số lượng giảm
            
        Returns:
            Giá trị mới sau khi giảm
            
        Example:
            >>> count = state_mgr.decrement("processed_count")
        """
        pass
    
    @staticmethod
    def flatten_list(lst: list) -> dict:
        """Flatten a list of dictionaries into a single dictionary with indexed keys."""
        if lst is None or not isinstance(lst, list):
            return {}
        ret = {}
        for i, item in enumerate(lst):
            if not isinstance(item, dict):
                continue
            for k, v in item.items():
                ret[f"{k}.{i}"] = v
        return ret
    
    @staticmethod
    def restore_list(data: dict, return_type: int = 0) -> list:
        """
        Restore a list of dictionaries from a flattened dictionary with indexed keys.

        Args:
            data: Flattened dictionary
            return_type: 0 - list of dicts, 1 - dict with index as key

        Returns:
            Restored list of dictionaries or dict of dictionaries
        """
        if data is None or not isinstance(data, dict):
            return []
        pattern = re.compile(r'^(?P<key>.+?)\.(?P<index>\d+)$')
        temp = {}
        for k, v in data.items():
            match = pattern.match(k)
            if not match:
                continue
            key = match.group('key')
            index = int(match.group('index'))
            if index not in temp:
                temp[index] = {}
            temp[index][key] = v
        
        if return_type == 1:
            return temp
        
        ret = []
        for i in sorted(temp.keys()):
            ret.append(temp[i])
        return ret
    

class InMemoryStateManager(StateManager):    
    def __init__(self):
        self._store: Dict[str, Any] = {}
        self._lock = threading.Lock()
    
    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._store.get(key, default)
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        with self._lock:
            self._store[key] = value
            # Note: TTL không được implement trong in-memory version
            # Sẽ được implement trong RedisStateManager
            return True
    
    def set_json(self, state_key: str, json_key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set a key inside a JSON object stored at state_key."""
        if state_key is None or json_key is None:
            return False
        with self._lock:
            obj = self._store.get(state_key, {})
            if not isinstance(obj, dict):
                obj = {}
            obj[json_key] = value
            self._store[state_key] = obj
            # TTL not implemented in in-memory version
            return True

    def get_json(self, state_key: str, json_key: str, default: Any = None) -> Any:
        """Get a key inside a JSON object stored at state_key."""
        if state_key is None or json_key is None:
            return default
        with self._lock:
            obj = self._store.get(state_key, {})
            if not isinstance(obj, dict):
                return default
            return obj.get(json_key, default)

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False
    
    def exists(self, key: str) -> bool:
        with self._lock:
            return key in self._store
    
    def clear(self) -> bool:
        with self._lock:
            self._store.clear()
            return True
    
    def get_all(self, pattern: str = "*") -> Dict[str, Any]:
        with self._lock:
            if pattern == "*":
                return self._store.copy()
            
            # Simple pattern matching (only supports prefix*)
            if pattern.endswith("*"):
                prefix = pattern[:-1]
                return {
                    k: v for k, v in self._store.items()
                    if k.startswith(prefix)
                }
            
            return {}
    
    def increment(self, key: str, amount: int = 1) -> int:
        with self._lock:
            current = self._store.get(key, 0)
            new_value = current + amount
            self._store[key] = new_value
            return new_value

    def decrement(self, key: str, amount: int = 1) -> int:
        with self._lock:
            current = self._store.get(key, 0)
            new_value = current - amount
            self._store[key] = new_value
            return new_value



class RedisStateManager(StateManager):
    """
    Redis-backed State Manager

    Features:
    - Atomic operations (INCR, DECR, APPEND, etc.)
    - Key pattern matching with SCAN (avoid blocking)
    - Queue/List operations (LPUSH, RPOP, LRANGE, etc.)
    - Automatic serialization/deserialization
    - TTL support with expiration
    """
    
    def __init__(self, redis_client=None, namespace: str = "cip"):
        """
        Args:
            redis_client: Redis connection instance
                         Nếu None, sẽ tự tạo từ config
            namespace: Prefix cho tất cả keys (default: "cip")
        """
        if redis_client is None:
            from .config import settings as cfg
            redis_client = redis.Redis(
                host=cfg.redis.host,
                port=cfg.redis.port,
                db=cfg.redis.db,
                password=cfg.redis.password,
                decode_responses=True
            )
        
        self.redis = redis_client
        self.namespace = namespace
    
    def _make_key(self, key: str) -> str:
        """Thêm namespace prefix vào key"""
        if not key.startswith(f"{self.namespace}:"):
            return f"{self.namespace}:{key}"
        return key
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Lấy giá trị từ Redis.
        
        Args:
            key: State key
            default: Giá trị mặc định nếu key không tồn tại
            
        Returns:
            Giá trị hoặc default
        """
        try:
            full_key = self._make_key(key)
            value = self.redis.get(full_key)
            return value if value is not None else default
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis GET error for key '{key}': {e}")
            return default

    def get_json(self, state_key: str, json_key: str, default: Any = None) -> Any:
        """
        Lấy một key bên trong object JSON lưu trong Redis.
        
        Args:
            state_key: State key chứa JSON object
            json_key: Key bên trong JSON object
            default: Giá trị mặc định nếu không tồn tại
            
        Returns:
            Giá trị hoặc default
        """
        try:
            full_key = self._make_key(state_key)
            json_str = self.redis.get(full_key)
            if json_str is None:
                return default
            obj = json.loads(json_str)
            if not isinstance(obj, dict):
                return default
            return obj.get(json_key, default)
        except (redis.RedisError, json.JSONDecodeError) as e:
            from .config import logger
            logger.error(f"Redis GET_JSON error for key '{state_key}.{json_key}': {e}")
            return default

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Lưu giá trị vào Redis.
        
        Args:
            key: State key
            value: Giá trị cần lưu
            ttl: Time-to-live in seconds (optional)
            
        Returns:
            True nếu thành công
        """
        try:
            full_key = self._make_key(key)
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            
            if ttl is not None:
                return self.redis.setex(full_key, ttl, value)
            else:
                return self.redis.set(full_key, value)
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis SET error for key '{key}': {e}")
            return False
    
    def set_json(self, state_key: str, json_key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a key inside a JSON object stored at state_key.
        
        Args:
            state_key: State key containing JSON object
            json_key: Key inside the JSON object
            value: Value to set
            ttl: Time-to-live in seconds (optional)
            
        Returns:
            True if successful
        """
        try:
            full_key = self._make_key(state_key)
            json_str = self.redis.get(full_key)
            if json_str is None:
                obj = {}
            else:
                obj = json.loads(json_str)
                if not isinstance(obj, dict):
                    obj = {}
            obj[json_key] = value
            new_json_str = json.dumps(obj)
            if ttl is not None:
                return self.redis.setex(full_key, ttl, new_json_str)
            else:
                return self.redis.set(full_key, new_json_str)
        except (redis.RedisError, json.JSONDecodeError) as e:
            from .config import logger
            logger.error(f"Redis SET_JSON error for key '{state_key}.{json_key}': {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        Xóa key khỏi Redis.
        
        Args:
            key: State key cần xóa
            
        Returns:
            True nếu key tồn tại và bị xóa
        """
        try:
            full_key = self._make_key(key)
            return self.redis.delete(full_key) > 0
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis DELETE error for key '{key}': {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """
        Kiểm tra key có tồn tại không.
        
        Args:
            key: State key
            
        Returns:
            True nếu key tồn tại
        """
        try:
            full_key = self._make_key(key)
            return self.redis.exists(full_key) > 0
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis EXISTS error for key '{key}': {e}")
            return False
    
    def clear(self) -> bool:
        """
        Xóa tất cả keys với namespace prefix.
        
        Returns:
            True nếu thành công
        """
        try:
            pattern = f"{self.namespace}:*"
            pipe = self.redis.pipeline()
            
            for key in self.redis.scan_iter(match=pattern, count=100):
                pipe.delete(key)
            
            pipe.execute()
            return True
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis CLEAR error: {e}")
            return False
    
    def get_all(self, pattern: str = "*") -> Dict[str, Any]:
        """
        Lấy tất cả keys matching pattern (sử dụng SCAN để tránh blocking).
        
        Args:
            pattern: Key pattern (e.g., "run:*", "step:*")
            
        Returns:
            Dict của tất cả matching keys
        """
        try:
            if pattern == "*":
                full_pattern = f"{self.namespace}:*"
            else:
                if not pattern.startswith(f"{self.namespace}:"):
                    full_pattern = f"{self.namespace}:{pattern}"
                else:
                    full_pattern = pattern
            
            result = {}
            for key in self.redis.scan_iter(match=full_pattern, count=100):
                # Remove namespace prefix để return clean key
                clean_key = key[len(self.namespace)+1:] if key.startswith(f"{self.namespace}:") else key
                value = self.redis.get(key)
                result[clean_key] = value
            
            return result
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis GET_ALL error: {e}")
            return {}
    
    def increment(self, key: str, amount: int = 1) -> int:
        """
        Tăng giá trị counter (atomic operation).
        
        Args:
            key: Counter key
            amount: Số lượng tăng
            
        Returns:
            Giá trị mới sau khi tăng
        """
        try:
            full_key = self._make_key(key)
            return self.redis.incrby(full_key, amount)
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis INCREMENT error for key '{key}': {e}")
            return 0
    
    def decrement(self, key: str, amount: int = 1) -> int:
        """
        Giảm giá trị counter (atomic operation).
        
        Args:
            key: Counter key
            amount: Số lượng giảm
            
        Returns:
            Giá trị mới sau khi giảm
        """
        try:
            full_key = self._make_key(key)
            return self.redis.decrby(full_key, amount)
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis DECREMENT error for key '{key}': {e}")
            return 0
    
    def append(self, key: str, value: str) -> int:
        """
        Append string value vào key.
        
        Args:
            key: State key
            value: String cần append
            
        Returns:
            Độ dài string sau append
        """
        try:
            full_key = self._make_key(key)
            return self.redis.append(full_key, value)
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis APPEND error for key '{key}': {e}")
            return 0
    
    def get_ttl(self, key: str) -> int:
        """
        Lấy TTL (time-to-live) của key.
        
        Args:
            key: State key
            
        Returns:
            TTL in seconds, -1 nếu không có expiration, -2 nếu không tồn tại
        """
        try:
            full_key = self._make_key(key)
            return self.redis.ttl(full_key)
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis TTL error for key '{key}': {e}")
            return -2
    
    def expire(self, key: str, seconds: int) -> bool:
        """
        Set expiration time cho key.
        
        Args:
            key: State key
            seconds: Seconds cho TTL
            
        Returns:
            True nếu thành công
        """
        try:
            full_key = self._make_key(key)
            return self.redis.expire(full_key, seconds)
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis EXPIRE error for key '{key}': {e}")
            return False
    
    # =========================================================================
    # CONNECTION & HEALTH CHECK
    # =========================================================================
    
    def ping(self) -> bool:
        """
        Kiểm tra kết nối Redis.
        
        Returns:
            True nếu kết nối thành công
        """
        try:
            return self.redis.ping()
        except redis.RedisError:
            return False
    
    def get_info(self) -> Dict[str, Any]:
        """
        Lấy thông tin Redis server.
        
        Returns:
            Dict chứa server info
        """
        try:
            info = self.redis.info()
            return {
                'redis_version': info.get('redis_version'),
                'connected_clients': info.get('connected_clients'),
                'used_memory_human': info.get('used_memory_human'),
                'uptime_in_seconds': info.get('uptime_in_seconds'),
                'keys': self.redis.dbsize()
            }
        except redis.RedisError as e:
            from .config import logger
            logger.error(f"Redis GET_INFO error: {e}")
            return {}




class RedisQueue:
    """
    Simple Redis-backed Queue using List commands.
    """
    
    def __init__(self, redis_client: redis.Redis=None, queue_key: str="cip_queue"):
        if redis_client is None:
            try:
                from .config import settings as cfg
                redis_client = redis.Redis(
                    host=cfg.redis.host,
                    port=cfg.redis.port,
                    db=cfg.redis.db,
                    password=cfg.redis.password,
                    decode_responses=True
                )
            except ImportError:
                raise ValueError("Redis client must be provided or config module must be available.")
        
        self.redis = redis_client
        self.queue_key = queue_key
    
    def push(self, *values) -> int:
        """Push values vào queue (RPUSH)."""
        serialized = []
        for v in values:
            if isinstance(v, (dict, list)):
                serialized.append(json.dumps(v))
            else:
                serialized.append(str(v))
        
        return self.redis.rpush(self.queue_key, *serialized)
    
    def pop(self, timeout: int = 0) -> Optional[Any]:
        """Pop value từ queue (LPOP)."""
        if timeout > 0:
            value = self.redis.blpop(self.queue_key, timeout=timeout)
            if value:
                return value[1]
            return None
        else:
            return self.redis.lpop(self.queue_key)
    
    def length(self) -> int:
        """Lấy độ dài queue."""
        return self.redis.llen(self.queue_key)
    
    def range(self, start: int = 0, end: int = -1) -> list:
        """Lấy range values từ queue."""
        return self.redis.lrange(self.queue_key, start, end)
    
    def clear(self) -> bool:
        """Xóa toàn bộ queue."""
        return self.redis.delete(self.queue_key) > 0




# =============================================================================
# HELPER: State Keys Constants
# =============================================================================

class StateKeys:
    """
    Định nghĩa tất cả state keys để tránh typo và dễ refactor.
    
    Usage:
        >>> state_mgr.set(StateKeys.CURRENT_RUN, run_data)
        >>> run = state_mgr.get(StateKeys.CURRENT_RUN)
    """
    
    FILTERED_CNT = "filtered_count"

    # Run state
    CURRENT_RUN = "current_run"
    LAST_RUN = "last_run"
    IS_NEW_RUN = "is_new_run"  # Flag to indicate if current run is newly created
    LAST_STEP_RECORD = "lstep_record"
    
    # Data rows
    LAST_DATA_ROW = "last_data_row"
    LAST_RUN_ROW = "last_run_row"
    LAST_TREND_ROW = "last_trend_row"

    # Data cache and shared
    DATA_SHARED = "cip_shared"
    DATA_CACHE = "cip_cache"
    
    @classmethod
    def run_key(cls, run_id: int) -> str:
        """Generate key cho specific run"""
        return f"run:{run_id}"
    
    @classmethod
    def step_key(cls, run_id: int, step_num: int) -> str:
        """Generate key cho specific step"""
        return f"step:{run_id}:{step_num}"

