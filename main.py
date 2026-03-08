from flight_performance_analytics_pipeline.logging.logging import Logger


def main() -> None:
    log = Logger()
    log.info("Hello from flight-performance-analytics-pipeline!")


if __name__ == "__main__":
    main()
