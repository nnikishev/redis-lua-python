

# Product Stock Reservation Controller

Проект предоставляет контроллер для управления резервированием товарных запасов с использованием Redis. Основная задача — обеспечение атомарности операций резервирования посредством выполнения Lua-скриптов.

## Основные возможности

- **Атомарные операции:** Использование Lua-скриптов в Redis гарантирует целостность данных при резервировании.
- **Управление стоками:** Перемещение товаров из статуса "доступно" в статус "зарезервировано".
- **Гибкая конфигурация:** Настройки подключения к Redis вынесены в модуль `settings`.

## Требования

Для работы необходим Python и библиотека `redis`:

```bash
pip install redis
```

# Запуск тестов (только состояния)
pytest test_controller.py::TestProductStockLogControllerStates -v -s

# Запуск с параллельным выполнением
pytest test_controller.py -v -s

# Результаты работы

```powershell
test_controller.py::TestProductStockLogControllerStates::test_key_not_exist_both_keys_missing PASSED
test_controller.py::TestProductStockLogControllerStates::test_key_not_exist_only_available_exists PASSED
test_controller.py::TestProductStockLogControllerStates::test_key_not_exist_only_reserved_exists PASSED
test_controller.py::TestProductStockLogControllerStates::test_enough_amount_state PASSED
test_controller.py::TestProductStockLogControllerStates::test_enough_amount_exact_match PASSED
test_controller.py::TestProductStockLogControllerStates::test_not_enough_amount_state PASSED
test_controller.py::TestProductStockLogControllerStates::test_not_enough_amount_zero_available PASSED
test_controller.py::TestProductStockLogControllerConcurrency::test_parallel_reservations_success PASSED
test_controller.py::TestProductStockLogControllerConcurrency::test_parallel_reservations_exhausting PASSED
test_controller.py::TestProductStockLogControllerConcurrency::test_parallel_different_amounts PASSED
test_controller.py::TestProductStockLogControllerConcurrency::test_race_condition_check
Итог:
  Успешных операций: 10
  Неудачных операций: 490
  Всего попыток: 500
  Начальный available: 10
  Финальный available: 0
  Финальный reserved: 10
  Сумма: 10
PASSED
```