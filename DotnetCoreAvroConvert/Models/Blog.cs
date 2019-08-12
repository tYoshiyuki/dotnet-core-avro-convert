using Avro;
using Avro.Specific;
using Microsoft.Hadoop.Avro;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace DotnetCoreAvroConvert.Models
{
    [DataContract]
    public class Blog: ISpecificRecord
    {
        [DataMember]
        public int BlogId { get; set; }
        [DataMember]
        public string Name { get; set; }
        [DataMember]
        public string Author { get; set; }

        public Schema Schema => Schema.Parse(AvroSerializer.Create<Blog>().WriterSchema.ToString());

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return BlogId;
                case 1: return Name;
                case 2: return Author;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            };
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: BlogId = (int)fieldValue; break;
                case 1: Name = (string)fieldValue; break;
                case 2: Author = (string)fieldValue; break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            };
        }
    }
}
