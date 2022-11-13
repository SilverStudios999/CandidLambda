using System;
using System.Collections.Generic;
using System.Text;

namespace CandidLambda.Models
{
    public class TimeStampedDoc
    {
        public string id { get; set; }
        public string name { get; set; }
        public DateTime timestamp { get; set; } = DateTime.UtcNow;
    }
}
