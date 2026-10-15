from gossip.metrics import sample_load


def test_sample_load_returns_cpu_and_ram():
    load = sample_load()

    assert "cpu" in load
    assert "ram" in load

    assert 0 <= load["cpu"] <= 100
    assert 0 <= load["ram"] <= 100