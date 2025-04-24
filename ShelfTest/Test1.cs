using Sunlighter.OptionLib;
using Sunlighter.ShelfLib;
using Sunlighter.TypeTraitsLib;
using System.Collections.Immutable;

namespace ShelfTest
{
    [TestClass]
    public sealed class ShelfTesting
    {
        [TestMethod]
        public void TestShelf()
        {
            string shelfPath = Path.Combine
            (
                Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory),
                "ShelfTest.sqlite"
            );

            var traits = new ValueTupleTypeTraits<string, int>(StringTypeTraits.Value, Int32TypeTraits.Value);

            var adapter = Adapter<(string, int)>.Create(traits);

            ImmutableSortedDictionary<(string, int), ImmutableList<(string, int)>> dict =
                ImmutableSortedDictionary<(string, int), ImmutableList<(string, int)>>.Empty.WithComparers(adapter);

            void add((string, int) key, (string, int) value)
            {
                dict = dict.SetItem(key, dict.GetValueOrDefault(key, ImmutableList<(string, int)>.Empty).Add(value));
            }

            using (var shelf = Shelf.Create(shelfPath, traits, traits, CreateOpenMode.Create))
            { 
                Random r = new Random(0x43B2FF30);

                using (var transaction = shelf.BeginTransaction())
                {
                    for (int i = 0; i < 1000; ++i)
                    {
                        int j = r.Next(1000);

                        (string, int) z1 = ("red", i);
                        (string, int) z2 = ("blue", j);

                        shelf.SetValue(AddReplaceMode.Add, z1, z2);
                        shelf.SetValue(AddReplaceMode.AddOrReplace, z2, z1);
                        add(z1, z2);
                        add(z2, z1);
                    }
                    transaction.Commit();
                }

                Assert.IsTrue(shelf.Count == dict.Count);

                foreach (var key in shelf.GetKeys(0, null))
                {
                    Assert.IsTrue(dict.ContainsKey(key));

                    //System.Diagnostics.Debug.WriteLine(traits.ToDebugString(key));
                }

                foreach(var key in dict.Keys)
                {
                    bool found = false;
                    foreach(var value in dict[key])
                    {
                        Option<(string, int)> valueOpt = shelf.TryGetValue(key);
                        if (valueOpt.HasValue) found = true;
                    }
                    Assert.IsTrue(found, $"Key {traits.ToDebugString(key)} not found in shelf");
                }
            }

            File.Delete(shelfPath);
        }
    }
}
