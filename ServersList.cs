using System;
using System.Collections.Generic;
using System.Configuration;
using System.Xml.Serialization;

namespace BulkQuery
{
    [Serializable]
    [SettingsSerializeAs(SettingsSerializeAs.Xml)]
    public class ServersList
    {
        [XmlArray("Servers")]
        public List<ServerDefinition> Servers { get; set; } = new List<ServerDefinition>();
    }
}
