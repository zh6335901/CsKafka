namespace Microsoft.Extensions.Logging
{
    internal static class LoggerExtensions
    {
        public static void LogInformationIfEnabled<T>(
            this ILogger logger, 
            string message, 
            T arg)
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(message, arg);
        }

        public static void LogInformationIfEnabled<T1, T2>(
            this ILogger logger, 
            string message, 
            T1 arg1, 
            T2 arg2)
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(message, arg1, arg2);
        }

        public static void LogInformationIfEnabled<T1, T2, T3>(
            this ILogger logger,
            string message,
            T1 arg1,
            T2 arg2,
            T3 arg3)
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(message, arg1, arg2, arg3);
        }

        public static void LogInformationIfEnabled<T1, T2, T3, T4>(
            this ILogger logger,
            string message,
            T1 arg1,
            T2 arg2,
            T3 arg3,
            T4 arg4)
        {
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(message, arg1, arg2, arg3, arg4);
        }
    }
}
