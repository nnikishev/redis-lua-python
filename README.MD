# Запуск тестов
pytest test_controller.py -v

# Запуск с параллельным выполнением
pytest test_controller.py -v -n auto

# Только быстрые тесты (без интеграционных)
pytest test_controller.py -v -m "not integration"