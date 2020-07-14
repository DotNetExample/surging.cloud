using Newtonsoft.Json;
using Surging.Core.CPlatform.Transport.Implementation;
using System;

namespace Surging.Core.CPlatform.Runtime.Session
{
    public class RpcContextSession : SurgingSessionBase
    {
        internal RpcContextSession()
        {
        }

        public override long? UserId
        {
            get
            {
                var userId = RpcContext.GetContext().GetAttachment("userId");
                if (userId != null) 
                {
                    return Convert.ToInt64(userId);
                }
                return null;
            }
        }

        public override string UserName
        {
            get
            {
                var userName = RpcContext.GetContext().GetAttachment("userName");
                if (userName != null) 
                {
                    return userName.ToString();
                }
                return null;
            }
        }
    }
}
