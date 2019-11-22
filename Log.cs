
using System.Linq;

namespace Raft
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public struct LogIndex
    {
        public static readonly LogIndex Invalid = new LogIndex(-1);

        internal int N;

        public LogIndex(int n)
        {
            N = n;
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public static bool operator ==(LogIndex a, LogIndex b)
        {
            return a.N == b.N;
        }

        public static bool operator !=(LogIndex a, LogIndex b)
        {
            return a.N != b.N;
        }

        public static bool operator >=(LogIndex a, LogIndex b)
        {
            return a.N >= b.N;
        }

        public static bool operator <=(LogIndex a, LogIndex b)
        {
            return a.N <= b.N;
        }

        public static bool operator >(LogIndex a, LogIndex b)
        {
            return a.N > b.N;
        }

        public static bool operator <(LogIndex a, LogIndex b)
        {
            return a.N < b.N;
        }
    }

    public struct Term
    {
        public static readonly Term Invalid = new Term(-1);

        internal int N;

        public Term(int n)
        {
            N = n;
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public static bool operator ==(Term a, Term b)
        {
            return a.N == b.N;
        }

        public static bool operator !=(Term a, Term b)
        {
            return a.N != b.N;
        }

        public static bool operator >=(Term a, Term b)
        {
            return a.N >= b.N;
        }

        public static bool operator <=(Term a, Term b)
        {
            return a.N <= b.N;
        }

        public static bool operator >(Term a, Term b)
        {
            return a.N > b.N;
        }

        public static bool operator <(Term a, Term b)
        {
            return a.N < b.N;
        }
    }

    internal interface ILogEntry<TWriteOp>
    {
        LogIndex Index { get; set; }
        Term Term { get; set; }
        TWriteOp Operation { get; set; }
    }

    internal interface ILog<TWriteOp>
    {
        int Length { get; }
        Task<bool> WriteAsync(ILogEntry<TWriteOp> entry);
        ILogEntry<TWriteOp> Get(LogIndex index);
    }

    public class LogEntry 
    {
        public LogIndex Index { get; set; }
        public Term Term { get; set; }
        public string Operation { get; set; }
    }

    public class Log 
    {
        public int Length { get; }
        public List<LogEntry> LogEntries { get; set; }
        public void WriteAsync(LogEntry entry)
        {
            LogEntries.Add(entry);
        } 

        public LogEntry Get(LogIndex index)
        {
            var entry = LogEntries.First(e => e.Index == index);
            return entry;
        }

        public Log()
        {
            this.LogEntries = new List<LogEntry>();
            this.Length = 0;
        }
    }

    internal class Log<TWriteOp> : ILog<TWriteOp>
    {
        Config _config;
        List<ILogEntry<TWriteOp>> _log = new List<ILogEntry<TWriteOp>>();

        public Log(Config config)
        {
            _config = config;
        }

        public Task<bool> WriteAsync(ILogEntry<TWriteOp> entry)
        {
            if (entry.Index.N > _log.Count)
                throw new InvalidOperationException("too far ahead");

            return Task.FromResult(true);
        }

        public ILogEntry<TWriteOp> Get(LogIndex index)
        {
            return _log[index.N];
        }

        public int Length
        {
            get { return _log.Count; }
        }
    }
}
