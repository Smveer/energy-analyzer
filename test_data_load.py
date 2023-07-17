import pytest
from EnergyAnalyzer import EnergyAnalyzer
from SessionBuilder import SessionBuilder


@pytest.fixture
def session_builder() -> SessionBuilder:
    return SessionBuilder()


@pytest.fixture
def file_name() -> str:
    return "energy_data/test.csv"


def test_load_data_from_csv(
        session_builder: SessionBuilder,
        file_name: str
):
    energy_analyzer = EnergyAnalyzer(
        session=session_builder
    )
    energy_analyzer.load_data_from_csv(
        file_name=file_name
    )
    assert energy_analyzer.energy_data.count() == 5
    assert energy_analyzer.energy_data.columns == ['id', 'name', 'age', 'city', 'country']
