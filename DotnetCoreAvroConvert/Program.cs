using Avro;
using Avro.File;
using Avro.Specific;
using DotnetCoreAvroConvert.Models;
using Microsoft.Hadoop.Avro;
using System;
using System.Collections.Generic;

namespace DotnetCoreAvroConvert
{
    class Program
    {
        static void Main(string[] args)
        {
            var schema = Schema.Parse(AvroSerializer.Create<Blog>().WriterSchema.ToString());

            var inputs = new List<Blog>
            {
                new Blog { BlogId = 101, Name = "Tanaka", Author = "One" },
                new Blog { BlogId = 201, Name = "Sato", Author = "Two" },
                new Blog { BlogId = 301, Name = "Suzuki", Author = "Three" }
            };

            var writer = new SpecificDatumWriter<Blog>(schema);
            using (var fw = DataFileWriter<Blog>.OpenWriter(writer, "./blog.avro"))
            {
                foreach(var blog in inputs)
                {
                    fw.Append(blog);
                }
            }

            var outputs = new List<Blog>();
            using (var fr = DataFileReader<Blog>.OpenReader("./blog.avro"))
            {
                while (fr.HasNext())
                {
                    outputs.Add(fr.Next());
                }
            }

            foreach(var b in outputs)
            {
                Console.WriteLine("----- Avro → POCO 変換後 -----");
                Console.WriteLine($"{b.BlogId} {b.Name} {b.Author}");
            }
        }
    }
}
