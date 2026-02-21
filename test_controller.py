import pytest
import redis
from redis.commands.core import Script
from threading import Thread, Lock
import concurrent.futures
import time
from enum import IntEnum
import sys
import os

# Добавляем путь к проекту для импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from redis_controller import ProductStockLogController
from script import lua_reservation_script
from enums import ProductAmountCacheResults
import settings


# ============================================================================
# FIXTURES - Общие фикстуры для всех тестов
# ============================================================================

@pytest.fixture
def controller():
    ctrl = ProductStockLogController()
    return ctrl


@pytest.fixture
def clean_redis():
    """Фикстура для очистки Redis перед и после тестов"""
    test_redis = redis.StrictRedis(
        host=settings.REDIS_HOST, 
        port=settings.REDIS_PORT, 
        db=15
    )
    test_redis.flushdb()
    yield test_redis
    test_redis.flushdb()  # Очищаем после теста


# ============================================================================
# ТЕСТЫ СОСТОЯНИЙ - Проверка всех возможных результатов из Enum
# ============================================================================

class TestProductStockLogControllerStates:
    """
    Тесты для проверки всех состояний из ProductAmountCacheResults:
    1. KEY_NOT_EXIST - когда ключи не существуют
    2. ENOUGH_AMOUNT - когда достаточно товара
    3. NOT_ENOUGH_AMOUNT - когда недостаточно товара
    """
    
    def test_key_not_exist_both_keys_missing(self, controller, clean_redis):
        """
        Тест состояния KEY_NOT_EXIST (1)
        Оба ключа отсутствуют в Redis
        """
        available_key = "test:product:1:available"
        reserved_key = "test:product:1:reserved"
        required_amount = 5
        
        # Убеждаемся что ключей действительно нет
        assert clean_redis.exists(available_key) == 0
        assert clean_redis.exists(reserved_key) == 0
        
        result, current_amount = controller.execute_lua_reservation_script(
            available_key, reserved_key, required_amount
        )
        
        assert result == ProductAmountCacheResults.KEY_NOT_EXIST
        assert current_amount is None
        
    def test_key_not_exist_only_available_exists(self, controller, clean_redis):
        """
        Тест состояния KEY_NOT_EXIST (1)
        Только available ключ существует, reserved отсутствует
        """
        available_key = "test:product:2:available"
        reserved_key = "test:product:2:reserved"
        required_amount = 5
        
        # Создаем только available ключ
        clean_redis.set(available_key, 10)
        
        result, current_amount = controller.execute_lua_reservation_script(
            available_key, reserved_key, required_amount
        )
        
        assert result == ProductAmountCacheResults.KEY_NOT_EXIST
        assert current_amount is None
        # Проверяем что available не изменился
        assert int(clean_redis.get(available_key)) == 10
        
    def test_key_not_exist_only_reserved_exists(self, controller, clean_redis):
        """
        Тест состояния KEY_NOT_EXIST (1)
        Только reserved ключ существует, available отсутствует
        """
        available_key = "test:product:3:available"
        reserved_key = "test:product:3:reserved"
        required_amount = 5
        
        # Создаем только reserved ключ
        clean_redis.set(reserved_key, 0)
        
        result, current_amount = controller.execute_lua_reservation_script(
            available_key, reserved_key, required_amount
        )
        
        assert result == ProductAmountCacheResults.KEY_NOT_EXIST
        assert current_amount is None
        
    def test_enough_amount_state(self, controller, clean_redis):
        """
        Тест состояния ENOUGH_AMOUNT (2)
        Достаточно товара для резервации
        """
        available_key = "test:product:4:available"
        reserved_key = "test:product:4:reserved"
        initial_available = 10
        initial_reserved = 0
        required_amount = 5
        
        # Устанавливаем начальные значения
        clean_redis.set(available_key, initial_available)
        clean_redis.set(reserved_key, initial_reserved)
        
        result, current_amount = controller.execute_lua_reservation_script(
            available_key, reserved_key, required_amount
        )
        
        assert result == ProductAmountCacheResults.ENOUGH_AMOUNT
        assert current_amount == 5
        
        # Проверяем что значения обновились атомарно
        assert int(clean_redis.get(available_key)) == initial_available - required_amount
        assert int(clean_redis.get(reserved_key)) == initial_reserved + required_amount
        
    def test_enough_amount_exact_match(self, controller, clean_redis):
        """
        Тест состояния ENOUGH_AMOUNT (2)
        Товара ровно столько сколько нужно
        """
        available_key = "test:product:5:available"
        reserved_key = "test:product:5:reserved"
        initial_available = 5
        initial_reserved = 2
        required_amount = 5
        
        clean_redis.set(available_key, initial_available)
        clean_redis.set(reserved_key, initial_reserved)
        
        result, current_amount = controller.execute_lua_reservation_script(
            available_key, reserved_key, required_amount
        )
        
        assert result == ProductAmountCacheResults.ENOUGH_AMOUNT
        assert int(clean_redis.get(available_key)) == 0
        assert int(clean_redis.get(reserved_key)) == initial_reserved + required_amount
        
    def test_not_enough_amount_state(self, controller, clean_redis):
        """
        Тест состояния NOT_ENOUGH_AMOUNT (3)
        Недостаточно товара для резервации
        """
        available_key = "test:product:6:available"
        reserved_key = "test:product:6:reserved"
        initial_available = 3
        initial_reserved = 1
        required_amount = 5
        
        clean_redis.set(available_key, initial_available)
        clean_redis.set(reserved_key, initial_reserved)
        
        result, current_amount = controller.execute_lua_reservation_script(
            available_key, reserved_key, required_amount
        )
        
        assert result == ProductAmountCacheResults.NOT_ENOUGH_AMOUNT
        assert current_amount == initial_available
        
        # Проверяем что значения не изменились
        assert int(clean_redis.get(available_key)) == initial_available
        assert int(clean_redis.get(reserved_key)) == initial_reserved
        
    def test_not_enough_amount_zero_available(self, controller, clean_redis):
        """
        Тест состояния NOT_ENOUGH_AMOUNT (3)
        Нет доступного товара
        """
        available_key = "test:product:7:available"
        reserved_key = "test:product:7:reserved"
        initial_available = 0
        initial_reserved = 5
        required_amount = 1
        
        clean_redis.set(available_key, initial_available)
        clean_redis.set(reserved_key, initial_reserved)
        
        result, current_amount = controller.execute_lua_reservation_script(
            available_key, reserved_key, required_amount
        )
        
        assert result == ProductAmountCacheResults.NOT_ENOUGH_AMOUNT
        assert current_amount == 0


# ============================================================================
# ТЕСТЫ С ПАРАЛЛЕЛЬНЫМ ВЫПОЛНЕНИЕМ - Проверка атомарности и thread-safety
# ============================================================================

class TestProductStockLogControllerConcurrency:
    """
    Тесты для проверки работы с множеством потоков
    Проверяем что Lua скрипт обеспечивает атомарность операций
    """
    
    def test_parallel_reservations_success(self, controller, clean_redis):
        """
        Тест параллельных успешных резерваций
        Множество потоков резервируют товар, суммарно не превышая лимит
        """
        available_key = "test:concurrent:1:available"
        reserved_key = "test:concurrent:1:reserved"
        initial_available = 100
        num_threads = 10
        reservations_per_thread = 5
        total_expected = num_threads * reservations_per_thread
        
        # Устанавливаем начальные значения
        clean_redis.set(available_key, initial_available)
        clean_redis.set(reserved_key, 0)
        
        def reserve_task(thread_id):
            """Задача для потока - сделать несколько резерваций"""
            local_results = []
            for i in range(reservations_per_thread):
                result, current_amount = controller.execute_lua_reservation_script(
                    available_key, reserved_key, 1
                )
                # Приводим результат к int для сравнения с enum
                result_val = int(result) if result is not None else None
                local_results.append(result_val)
                # Небольшая задержка для имитации реальной работы
                time.sleep(0.001)
            return local_results
        
        # Запускаем потоки
        all_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(reserve_task, i) for i in range(num_threads)]
            for future in concurrent.futures.as_completed(futures):
                all_results.extend(future.result())
        
        # Проверяем результаты
        successful = sum(1 for r in all_results if r == ProductAmountCacheResults.ENOUGH_AMOUNT)
        not_enough = sum(1 for r in all_results if r == ProductAmountCacheResults.NOT_ENOUGH_AMOUNT)
        key_not_exist = sum(1 for r in all_results if r == ProductAmountCacheResults.KEY_NOT_EXIST)
        
        assert successful == total_expected, f"Должно быть {total_expected} успешных, получено {successful}"
        assert not_enough == 0, f"Не должно быть нехватки, получено {not_enough}"
        assert key_not_exist == 0, f"Не должно быть отсутствия ключей, получено {key_not_exist}"
        
        # Проверяем финальные значения
        final_available = int(clean_redis.get(available_key))
        final_reserved = int(clean_redis.get(reserved_key))
        
        assert final_available == initial_available - total_expected
        assert final_reserved == total_expected
        
    def test_parallel_reservations_exhausting(self, controller, clean_redis):
        """
        Тест параллельных резерваций с исчерпанием стока
        Потоки пытаются зарезервировать больше чем есть
        """
        available_key = "test:concurrent:2:available"
        reserved_key = "test:concurrent:2:reserved"
        initial_available = 50
        num_threads = 20
        reservations_per_thread = 3  # Всего попыток: 60
        amount_per_reservation = 1
        
        clean_redis.set(available_key, initial_available)
        clean_redis.set(reserved_key, 0)
        
        def reserve_task(thread_id):
            """Задача для потока"""
            thread_success = 0
            thread_failures = 0
            for i in range(reservations_per_thread):
                result, current = controller.execute_lua_reservation_script(
                    available_key, reserved_key, amount_per_reservation
                )
                result_val = int(result) if result is not None else None
                
                if result_val == ProductAmountCacheResults.ENOUGH_AMOUNT:
                    thread_success += 1
                elif result_val == ProductAmountCacheResults.NOT_ENOUGH_AMOUNT:
                    thread_failures += 1
                time.sleep(0.001)
            return thread_success, thread_failures
        
        # Запускаем и собираем результаты
        total_success = 0
        total_failures = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(reserve_task, i) for i in range(num_threads)]
            for future in concurrent.futures.as_completed(futures):
                success, failures = future.result()
                total_success += success
                total_failures += failures
        
        # Проверяем
        total_attempts = num_threads * reservations_per_thread
        final_available = int(clean_redis.get(available_key))
        final_reserved = int(clean_redis.get(reserved_key))
        
        assert total_success == initial_available, f"Успешно: {total_success}, ожидалось: {initial_available}"
        assert total_success + total_failures == total_attempts, "Не все попытки учтены"
        assert final_available == 0, f"Остаток: {final_available}, должен быть 0"
        assert final_reserved == initial_available, f"Резерв: {final_reserved}, должен быть {initial_available}"
        
    def test_parallel_different_amounts(self, controller, clean_redis):
        """
        Тест параллельных резерваций с разными количествами
        """
        available_key = "test:concurrent:3:available"
        reserved_key = "test:concurrent:3:reserved"
        initial_available = 100
        
        clean_redis.set(available_key, initial_available)
        clean_redis.set(reserved_key, 0)
        
        amounts = [1, 2, 3, 4, 5]  # Разные суммы для резервации
        
        def reserve_task(thread_id):
            """Поток резервирует разные суммы"""
            total_reserved = 0
            for amount in amounts:
                result, current = controller.execute_lua_reservation_script(
                    available_key, reserved_key, amount
                )
                result_val = int(result) if result is not None else None
                
                if result_val == ProductAmountCacheResults.ENOUGH_AMOUNT:
                    total_reserved += amount
                time.sleep(0.001)
            return total_reserved
        
        # Запускаем 10 потоков
        total_reserved = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(reserve_task, i) for i in range(10)]
            for future in concurrent.futures.as_completed(futures):
                total_reserved += future.result()
        
        final_available = int(clean_redis.get(available_key))
        final_reserved = int(clean_redis.get(reserved_key))
        
        assert final_available == initial_available - total_reserved
        assert final_reserved == total_reserved
        assert final_available >= 0
        
    def test_race_condition_check(self, controller, clean_redis):
        """
        Тест для выявления состояний гонки
        Запускаем много потоков и проверяем что не возникает отрицательных значений
        """
        available_key = "test:race:available"
        reserved_key = "test:race:reserved"
        initial_available = 10
        num_threads = 50
        operations_per_thread = 10
        
        clean_redis.set(available_key, initial_available)
        clean_redis.set(reserved_key, 0)
        
        # Сбрасываем кэш скрипта
        controller.lua_reservation_script = None
        
        negative_detected = False
        inconsistent_state = False
        results_lock = Lock()
        
        # Счетчики для отслеживания
        total_success = 0
        total_failures = 0
        
        def race_test_task(thread_id):
            nonlocal negative_detected, inconsistent_state, total_success, total_failures
            
            for _ in range(operations_per_thread):
                # Пытаемся зарезервировать 1 единицу
                result, current = controller.execute_lua_reservation_script(
                    available_key, reserved_key, 1
                )
                result_val = int(result) if result is not None else None
                
                if result_val == ProductAmountCacheResults.ENOUGH_AMOUNT:
                    with results_lock:
                        total_success += 1
                elif result_val == ProductAmountCacheResults.NOT_ENOUGH_AMOUNT:
                    with results_lock:
                        total_failures += 1
                
                # Проверяем инвариант с использованием атомарной операции Redis
                # Используем pipeline для атомарного чтения обоих ключей
                pipe = clean_redis.pipeline()
                pipe.get(available_key)
                pipe.get(reserved_key)
                avail_val, res_val = pipe.execute()
                
                current_available = int(avail_val) if avail_val is not None else 0
                current_reserved = int(res_val) if res_val is not None else 0
                
                # Проверяем отрицательные значения
                if current_available < 0:
                    with results_lock:
                        negative_detected = True
                        print(f"Отрицательный остаток: {current_available}")
                
                # Проверяем инвариант суммы (допускаем погрешность из-за параллельных операций)
                # Сумма может быть меньше начальной, если некоторые операции еще не завершены
                if current_available + current_reserved > initial_available:
                    with results_lock:
                        inconsistent_state = True
                        print(f"Сумма больше начальной: {current_available + current_reserved} > {initial_available}")
        
        # Запускаем потоки
        threads = []
        for i in range(num_threads):
            t = Thread(target=race_test_task, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Проверяем итоговые значения
        final_available = int(clean_redis.get(available_key))
        final_reserved = int(clean_redis.get(reserved_key))
        
        print(f"\nИтог:")
        print(f"  Успешных операций: {total_success}")
        print(f"  Неудачных операций: {total_failures}")
        print(f"  Всего попыток: {num_threads * operations_per_thread}")
        print(f"  Начальный available: {initial_available}")
        print(f"  Финальный available: {final_available}")
        print(f"  Финальный reserved: {final_reserved}")
        print(f"  Сумма: {final_available + final_reserved}")
        
        # Проверяем что не было гонок
        assert not negative_detected, "Обнаружено отрицательное значение - есть race condition!"
        assert not inconsistent_state, "Обнаружено превышение начальной суммы!"
        
        # Проверяем финальное состояние
        assert final_available + final_reserved == initial_available, f"Инвариант суммы нарушен: {final_available} + {final_reserved} != {initial_available}"
        assert final_available >= 0, "Отрицательный остаток!"
        assert final_available == initial_available - total_success, f"Количество успешных операций не соответствует: {final_available} != {initial_available - total_success}"
