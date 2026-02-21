from script import lua_reservation_script
import settings
import redis
from redis.commands.core import Script


class ProductStockLogController:
    def __init__(self) -> None:
        self.redis_host = settings.REDIS_HOST
        self.redis_port = settings.REDIS_PORT
        self.redis_db = settings.REDIS_DB
        self.redis_client = self._get_redis_client()

    def _get_redis_client(self) -> redis.StrictRedis:
        redis_client = redis.StrictRedis(
            host=self.redis_host, 
            port=self.redis_port, 
            db=self.redis_db)
        return redis_client


    def _get_lua_reservation_script(self) -> Script:
        self.lua_reservation_script = self.redis_client.register_script(lua_reservation_script)
        return self.lua_reservation_script

    def execute_lua_reservation_script(
      self, 
      available_key: str, 
      reserved_key: str, 
      required_amount: int
    ) -> tuple[int, int | None]:
        lua_script: Script = self._get_lua_reservation_script()
        result, current_amount = lua_script(keys=[available_key, reserved_key], args=[required_amount])
        return result, current_amount