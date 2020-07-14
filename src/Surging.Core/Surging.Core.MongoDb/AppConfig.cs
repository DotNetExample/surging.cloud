using Microsoft.Extensions.Configuration;
using Surging.Core.MongoDb.Configurations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Surging.Core.MongoDb
{
    public class AppConfig
    {
        internal static string Path;
        internal static IConfigurationRoot Configuration { get; set; }

        public static IConfigurationSection GetSection(string name)
        {
            return Configuration?.GetSection(name);
        }

        public static MongoDbOption MongoDbOption { get; internal set; }

    }
}
