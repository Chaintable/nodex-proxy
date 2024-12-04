package log

func Sync() error {
	if err := logger.Sync(); err != nil {
		return err
	}
	if err := observabilityLogger.Sync(); err != nil {
		return err
	}
	return nil
}
