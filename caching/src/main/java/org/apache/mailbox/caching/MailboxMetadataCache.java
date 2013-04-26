package org.apache.mailbox.caching;

import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.mail.model.Mailbox;

public interface MailboxMetadataCache<Id> {

	public abstract long countMessagesInMailbox(Mailbox<Id> mailbox,
			MessageMapper<Id> underlying) throws MailboxException;

	public abstract long countUnseenMessagesInMailbox(Mailbox<Id> mailbox,
			MessageMapper<Id> underlying) throws MailboxException;

	public abstract Long findFirstUnseenMessageUid(Mailbox<Id> mailbox,
			MessageMapper<Id> underlying) throws MailboxException;

	public abstract long getLastUid(Mailbox<Id> mailbox,
			MessageMapper<Id> underlying) throws MailboxException;

	public abstract long getHighestModSeq(Mailbox<Id> mailbox,
			MessageMapper<Id> underlying) throws MailboxException;

	public abstract void invalidate(Mailbox<Id> mailbox);

//	public abstract void invalidate(MailboxPath mailboxPath);

}