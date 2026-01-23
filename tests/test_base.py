"""
Tests for the BaseBroker abstract class
"""
import pytest
from abc import ABC
from india_stocks_api.abstract_brokers.base import BaseBroker


class TestBaseBroker:
    """Test cases for BaseBroker abstract class"""

    def test_base_broker_is_abstract(self):
        """Test that BaseBroker cannot be instantiated directly"""
        with pytest.raises(TypeError):
            BaseBroker()

    def test_base_broker_is_abc(self):
        """Test that BaseBroker is an ABC"""
        assert issubclass(BaseBroker, ABC)

    def test_all_abstract_methods_are_defined(self):
        """Test that all required abstract methods are defined"""
        abstract_methods = {
            'logout',
            'get_order_book',
            'get_trade_book',
            'get_positions',
            'get_holdings',
            'place_order',
            'modify_order',
            'cancel_order',
            'get_quotes',
            'get_history',
            'get_depth',
            'calculate_margin'
        }
        
        # Get all abstract methods from BaseBroker
        actual_methods = set(BaseBroker.__abstractmethods__)
        assert actual_methods == abstract_methods

    def test_concrete_implementation_must_implement_all_methods(self):
        """Test that a concrete class must implement all abstract methods"""
        
        # Partial implementation should fail
        class IncompleteBroker(BaseBroker):
            def logout(self):
                pass
            
            def get_order_book(self):
                pass
        
        with pytest.raises(TypeError):
            IncompleteBroker()

    def test_complete_implementation_can_be_instantiated(self):
        """Test that a complete implementation can be instantiated"""
        
        class CompleteBroker(BaseBroker):
            def logout(self):
                return "logged out"
            
            def get_order_book(self):
                return []
            
            def get_trade_book(self):
                return []
            
            def get_positions(self):
                return []
            
            def get_holdings(self):
                return []
            
            def place_order(self, order_details):
                return "order_id_123"
            
            def modify_order(self, order_id, order_details):
                return order_id
            
            def cancel_order(self, order_id):
                return order_id
            
            def get_quotes(self, symbol: str, exchange: str):
                return {"symbol": symbol, "exchange": exchange}
            
            def get_history(self, instruments: str, interval: str, start_date: str, end_date: str):
                return []
            
            def get_depth(self, symbol: str, exchange: str):
                return {}
            
            def calculate_margin(self, positions):
                return 0.0
        
        # Should be able to instantiate
        broker = CompleteBroker()
        assert broker is not None
        assert broker.logout() == "logged out"
        assert broker.get_order_book() == []
        assert broker.place_order({}) == "order_id_123"
