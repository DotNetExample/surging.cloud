using Surging.Core.CPlatform.Exceptions;
using System;

namespace Surging.Core.CPlatform.Messages
{
    public static class RemoteInvokeResultMessageExtensions
    {
        public static bool IsSucceedRemoteInvokeCalled(this RemoteInvokeResultMessage message)
        {
            return message.StatusCode == StatusCode.Success
                || message.StatusCode == StatusCode.ValidateError
                || message.StatusCode == StatusCode.UserFriendly
                || message.StatusCode == StatusCode.BusinessError
                || message.StatusCode == StatusCode.UnAuthentication
                || message.StatusCode == StatusCode.UnAuthorized
                || message.StatusCode == StatusCode.DataAccessError

                ;
        }

        public static bool IsFailedRemoteInvokeCalled(this RemoteInvokeResultMessage message)
        {
            return message.StatusCode == StatusCode.CommunicationError
                || message.StatusCode == StatusCode.ServiceUnavailability
                ;
        }

        public static Exception GetExceptionByStatusCode(this RemoteInvokeResultMessage message)
        {
            Exception exception = null;
            switch (message.StatusCode)
            {
                case StatusCode.BusinessError:
                    exception = new BusinessException(message.ExceptionMessage);
                    break;
                case StatusCode.CommunicationError:
                    exception = new CommunicationException(message.ExceptionMessage);
                    break;
                case StatusCode.RequestError:
                case StatusCode.CPlatformError:
                case StatusCode.UnKnownError:
                    exception = new CPlatformException(message.ExceptionMessage, message.StatusCode);
                    break;
                case StatusCode.DataAccessError:
                    exception = new DataAccessException(message.ExceptionMessage);
                    break;
                case StatusCode.UnAuthentication:
                    exception = new UnAuthenticationException(message.ExceptionMessage);
                    break;
                case StatusCode.UnAuthorized:
                    exception = new UnAuthorizedException(message.ExceptionMessage);
                    break;
                case StatusCode.UserFriendly:
                    exception = new UserFriendlyException(message.ExceptionMessage);
                    break;
                case StatusCode.ValidateError:
                    exception = new ValidateException(message.ExceptionMessage);
                    break;
                default:
                    exception = new CPlatformException(message.ExceptionMessage, message.StatusCode);
                    break;
            }

            return exception;
        }
    }

}
